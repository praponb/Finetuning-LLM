from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-11-20"))) & F.col("job_title").isNotNull())
    .withColumn("email_prefix", F.substring("email", 1, 3))
    .withColumn("email_slug", F.regexp_replace("email", " ", "-"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("salary") >= 150000, "HIGH").otherwise("LOW").alias("salary_tier"),
        F.date_trunc("month", "hire_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.avg("salary").alias("avg_salary_2"),
    )
    .orderBy(F.col("salary_tier").desc())
)
