from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("email").like("%avi%"))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.count("*").alias("employee_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("salary_range") < 69)
)
