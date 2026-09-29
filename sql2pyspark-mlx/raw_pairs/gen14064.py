from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.lit("store_sale").alias("source"), "revenue")
    .union(
        df_subscriptions
        .filter(F.col("customer_id") == 23)
        .select(F.lit("subscription").alias("source"), "monthly_fee")
    )
    .distinct()
    .orderBy("source")
)
