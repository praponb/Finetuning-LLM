from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2023-10-03")
    .select(F.lit("subscription").alias("source"), "plan")
    .union(
        df_customers
        .filter(F.col("is_active"))
        .select(F.lit("customer").alias("source"), "email")
    )
    .distinct()
)
