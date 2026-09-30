from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-04-08").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .withColumn("discount_per_shipping_fee", (F.col("discount") / F.nullif("shipping_fee", F.lit(0))))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.15), 2))
)

result = (
    filtered
    .select("store_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("store_id", "channel")
    )
)
