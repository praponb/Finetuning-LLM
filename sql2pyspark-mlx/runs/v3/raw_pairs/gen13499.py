from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 94000)
    .groupBy(
        F.date_format("hire_date", "yyyy-MM").alias("hire_month"),
        "is_remote",
        "dept_id",
    )
    .agg(F.count("*").alias("row_count"))
)
