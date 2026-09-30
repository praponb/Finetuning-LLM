from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_employees
        .select(F.col("hire_date").alias("start_date"), F.lit("employee").alias("source"))
    )
    .distinct()
)
