from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Designer")
    .select(F.col("hire_date").alias("event_date"), F.lit("employee").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.col("status") == "CLOSED")
        .select(F.col("opened_date").alias("event_date"), F.lit("account").alias("source"))
    )
    .orderBy(F.col("event_date").desc())
)
