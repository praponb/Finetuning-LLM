from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())

prepared = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

unique_rows = (
    prepared
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    unique_rows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.sum("unit_price").alias("total_move_unit_price"),
        F.min("moved_at").alias("first_moved_at_2"),
    )
)

summarized_rows = (
    latest.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.total_move_unit_price",
        "st.first_moved_at_2",
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at", "st.last_moved_at_2")
)

result = (
    summarized_rows_2
    .filter(F.col("first_moved_at") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select(
        "id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "first_moved_at",
        "last_moved_at",
        F.coalesce(F.col("total_move_unit_price"), F.lit(0.0)).alias("total_move_unit_price_filled"),
        F.coalesce(F.col("total_move_unit_price"), F.lit(0.0)).alias("total_move_unit_price_filled_2"),
    )
    .orderBy(F.col("unit_price").desc())
)
