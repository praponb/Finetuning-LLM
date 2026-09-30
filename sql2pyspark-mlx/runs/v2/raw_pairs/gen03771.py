from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(~F.col("job_title").isin(["Manager", "Engineer", "Designer"]) & F.col("email").like("%@startup.io"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
)

employees_stats = (
    recent_employees
    .groupBy(
        F.when(F.col("salary") >= 155000, "high").when(F.col("salary") >= 46000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = employees_stats.orderBy(F.col("job_title_values").desc())
