from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("operator", "move_type").orderBy(F.col("unit_price").desc(), F.col("id").desc())

filtered_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("move_type") == "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 348).alias("qty_distance"),
    )
)

latest = (
    filtered_moves
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_2 = (
    latest
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_2
    .groupBy("warehouse_id", "sku_id")
    .pivot("operator", ["op_chan", "op_dew", "op_bee", "op_eve"])
    .agg(F.avg("qty"))
    .orderBy("warehouse_id", "sku_id")
)
