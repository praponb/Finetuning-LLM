from pyspark.sql import functions as F

joined = (
    df_employees
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.location"), F.col("b.dept_name"))
)

result = (
    joined
    .groupBy(
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
        F.when(F.col("salary") >= 128000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy(F.col("max_salary").desc())
)
