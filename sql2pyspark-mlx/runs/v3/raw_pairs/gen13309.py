from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("dept_id").isin([4, 8, 29, 33]))
    .withColumn("salary_band", F.when(F.col("salary") >= 156000, "High").when(F.col("salary") >= 44000, "Medium").otherwise("Low"))
)

hire_month_summary = (
    valid_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
    )
)

result = (
    hire_month_summary
    .filter(F.col("manager_salary") > 22)
    .select("hire_month", "manager_salary")
    .orderBy(F.col("hire_month").desc())
)
