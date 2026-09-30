from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").isNotNull())
    .select("start_date")
    .unionByName(
        df_customers
        .filter(F.col("signup_date").isNotNull())
        .select(F.col("signup_date").alias("start_date"))
    )
    .orderBy(F.col("start_date").desc())
)
