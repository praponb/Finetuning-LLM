from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select(F.lit("customer").alias("kind"), F.col("signup_date").alias("event_date"))
    .unionByName(
        df_accounts
        .filter(F.col("opened_date").isNotNull())
        .select(F.lit("account").alias("kind"), F.col("opened_date").alias("event_date"))
    )
    .orderBy(F.col("event_date").desc())
)
