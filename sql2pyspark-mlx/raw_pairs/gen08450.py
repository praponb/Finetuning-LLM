from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("discount").asc_nulls_first())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_first())

base = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("store"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("amount") >= 1740, "High").when(F.col("amount") >= 290, "Medium").otherwise("Low").alias("amount_tier"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount_tier",
        F.rank().over(w_running).alias("discount_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.percent_rank().over(w3).alias("shipping_fee_percent_rank"),
    )
)
