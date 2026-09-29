from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .unionByName(
        df_products
        .filter(F.array_contains("tags", "bestseller"))
        .select(F.col("cost").alias("monthly_fee"))
    )
)
