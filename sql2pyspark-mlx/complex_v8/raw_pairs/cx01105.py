from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("move_count").desc(), F.col("id").desc())

base_moves = (
    df_stock_moves
    .filter((F.col("unit_price").between(86.3, 436.9)) & (F.col("qty") <= 288))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

filtered = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") >= 4) & (F.col("st.unit_price").between(95.8, 330.9)), "left_anti")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

unique_rows = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .filter(F.month(F.col("last_moved_at")).isin(2, 3))
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "operator_len",
        "last_moved_at",
        "move_count",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)
