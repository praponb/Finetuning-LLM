from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select(F.lit("subscription").alias("origin"), "monthly_fee")
    .union(
        df_store_sales
        .filter(F.col("units") != 23)
        .select(F.lit("store_sale").alias("origin"), "revenue")
    )
    .distinct()
    .orderBy("monthly_fee")
)
