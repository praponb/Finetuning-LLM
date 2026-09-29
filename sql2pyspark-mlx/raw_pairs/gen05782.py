from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") < "2025-01-01") & F.col("dept_id").isin([9, 18, 27]))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        F.quarter("hire_date").alias("hire_quarter"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("employee_count"),
        F.count("*").alias("cnt"),
    )
)
