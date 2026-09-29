from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-01-01", "2025-03-08") & F.col("end_date").between("2024-09-01", "2025-01-07"))
    .groupBy(
        "plan",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(F.max("end_date").alias("last_end_date"))
)
