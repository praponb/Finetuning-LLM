from pyspark.sql import functions as F

valid_employees = df_employees.filter(~F.col("is_remote"))

agg = (
    valid_employees
    .groupBy(
        "job_title",
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = agg.orderBy(F.col("salary_range").desc(), "hire_cohort")
