from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())

base_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

unique_rows = (
    base_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .groupBy("warehouse_id")
    .pivot("move_type", ["OUTBOUND", "ADJUST"])
    .agg(F.max("unit_price_bucket").alias("max_v"), F.count("unit_price_bucket").alias("n"))
    .orderBy("warehouse_id")
)
