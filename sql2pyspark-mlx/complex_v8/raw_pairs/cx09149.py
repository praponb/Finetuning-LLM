from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_dedup = Window.partitionBy("sku_id").orderBy(F.col("warehouse_id_min_unit_price").desc(), F.col("id").desc())

prepared = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.dense_rank().over(w_all_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.min("unit_price").over(w_warehouse_id).alias("warehouse_id_min_unit_price"),
        F.max("unit_price").over(w_warehouse_id).alias("warehouse_id_max_unit_price"),
        F.ntile(5).over(w_all_by_unit_price).alias("unit_price_quintile"),
    )
)

unique_rows = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("move_move_type_count"),
    )
)

summarized_rows = (
    unique_rows.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    summarized_rows
    .groupBy("unit_price_quintile")
    .pivot("operator", ["op_ann", "op_eve", "op_bee", "op_dew"])
    .agg(F.min("warehouse_id_min_unit_price"))
    .orderBy("unit_price_quintile")
)
