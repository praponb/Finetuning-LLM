from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date", F.lit("subscription").alias("source"))
    .unionByName(
        df_employees
        .filter(~F.col("is_remote"))
        .select(F.col("hire_date").alias("end_date"), F.lit("employee").alias("source"))
    )
)
