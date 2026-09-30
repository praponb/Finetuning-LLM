from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNull() & F.col("hire_date").between("2023-03-01", "2024-12-01") & ~F.col("full_name").like("Oli%"))
    .select(
        "dept_id",
        "hire_date",
        F.date_trunc("MONTH", "hire_date").alias("hire_date_period"),
    )
)
