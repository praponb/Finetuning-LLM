from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([14, 20, 26]))
    .select(F.col("hire_date").alias("event_date"), F.lit("employee").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "basic")
        .select(
        F.col("end_date").alias("event_date"),
        F.lit("subscription").alias("source"),
    )
    )
)
