from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), F.col("id").desc())

base_moves = (
    df_stock_moves
    .filter((F.col("sku_id") >= 12) & (F.col("moved_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    base_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

unique_rows = (
    summarized_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .groupBy("move_count", "moved_at")
    .pivot("move_type", ["INBOUND", "OUTBOUND", "TRANSFER"])
    .agg(F.sum("unit_price").alias("amount"), F.count("unit_price").alias("n"))
)
