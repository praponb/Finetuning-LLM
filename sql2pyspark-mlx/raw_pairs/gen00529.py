from pyspark.sql import functions as F

valid_employees = df_employees.filter((F.col("salary") <= 124000) & F.col("manager_id").isNull())

summary = (
    valid_employees
    .groupBy(
        F.when(F.col("salary") >= 132000, "high").when(F.col("salary") >= 56000, "mid").otherwise("low").alias("salary_bucket"),
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    summary
    .filter(F.col("avg_salary") > 63000)
    .select("salary_bucket", "hire_cohort", "avg_salary")
)
