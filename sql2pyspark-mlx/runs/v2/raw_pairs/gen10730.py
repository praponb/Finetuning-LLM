from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "level",
        F.date_trunc("week", "hire_date").alias("hire_date_period"),
        F.col("salary").cast("int").alias("salary_int"),
    )
)
