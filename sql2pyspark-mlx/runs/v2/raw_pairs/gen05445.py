from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("dept_id").isin([5, 7, 26, 29]))
    .withColumn("job_title_upper", F.upper("job_title"))
)

summary = (
    filtered_employees
    .groupBy(
        F.when(F.col("salary") >= 163000, "high").when(F.col("salary") >= 45000, "mid").otherwise("low").alias("salary_bucket"),
        "job_title",
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum("salary").alias("total_salary"),
    )
)

result = summary.orderBy(F.col("salary_bucket").desc(), "stddev_salary")
