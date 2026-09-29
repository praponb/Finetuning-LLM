from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 34) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
    )
    .orderBy("shipping_fee_filled", "customer_id", "id")
    .limit(20)
)
