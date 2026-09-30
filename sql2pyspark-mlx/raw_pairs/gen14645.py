from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-30").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.date_add("order_date", 27).alias("order_date_plus_27d"),
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "amount", "discount", "order_date_plus_27d")
)
