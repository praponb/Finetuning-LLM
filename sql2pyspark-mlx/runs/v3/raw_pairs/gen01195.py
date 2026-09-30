from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_customers
        .filter(F.col("age") <= 35)
        .select(F.col("signup_date").alias("end_date"), F.lit("customer").alias("source"))
    )
    .orderBy(F.col("source").desc())
)
