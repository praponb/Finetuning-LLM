from pyspark.sql import functions as F

result = (
    df_employees
    .select("level", F.lit("employee").alias("origin"))
    .union(
        df_subscriptions
        .select("status", F.lit("subscription").alias("origin"))
    )
    .distinct()
)
