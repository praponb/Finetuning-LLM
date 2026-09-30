from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy(F.col("amount_with_shipping").desc(), "id")

base = (
    df_orders
    .filter(~F.col("channel").eqNullSafe(F.lit("web")))
    .select(
        "*",
        (F.col("shipping_fee") * F.col("amount")).alias("amount_with_shipping"),
        F.lower("channel").alias("channel_lower"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "status", "amount")
)
