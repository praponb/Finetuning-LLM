from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-02-28").cast("date")) & F.col("level").isNotNull())
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("N/A")))
)

result = (
    filtered
    .select("level")
    .exceptAll(
        df_employees
        .filter(F.col("level").isin(["L1", "L2"]))
        .select("level")
    )
)
