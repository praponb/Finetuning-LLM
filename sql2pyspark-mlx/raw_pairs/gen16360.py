from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "channel",
        F.col("discount").cast("int").alias("discount_int"),
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
    )
)
