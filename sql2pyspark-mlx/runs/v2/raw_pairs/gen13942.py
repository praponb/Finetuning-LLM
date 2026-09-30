from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") >= "2025-07-08") & F.col("hire_date").between("2024-06-01", "2025-06-12"))
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.count("*").alias("num_employees"),
    )
)
