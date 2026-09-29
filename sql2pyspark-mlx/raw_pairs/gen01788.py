from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "dept_id",
    )
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .orderBy("dept_id")
)
