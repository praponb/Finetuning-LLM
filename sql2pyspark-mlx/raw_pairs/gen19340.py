from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2024-10-01")
    .select(F.lit("subscription").alias("kind"), "monthly_fee")
    .unionByName(
        df_products
        .filter(F.col("supplier_id").isin([9, 22, 30, 38]))
        .select(F.lit("product").alias("kind"), F.col("price").alias("monthly_fee"))
    )
    .distinct()
    .orderBy(F.col("kind").desc())
)
