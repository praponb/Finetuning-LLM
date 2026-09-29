from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "dept_id",
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
    )
    .filter(F.col("manager_salary") < 70)
    .orderBy("hire_cohort", "manager_salary")
)
