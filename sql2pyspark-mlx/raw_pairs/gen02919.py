from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").isNotNull())
    .select("hire_date", F.lit("employee").alias("record_type"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").isNotNull())
        .select(
        F.col("end_date").alias("hire_date"),
        F.lit("subscription").alias("record_type"),
    )
    )
    .distinct()
)
