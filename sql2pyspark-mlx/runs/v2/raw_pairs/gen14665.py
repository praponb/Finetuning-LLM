from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.col("hire_date") <= "2024-10-01") & F.col("email").like("%@gmail.com"))
    .agg(
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy(F.col("max_salary").desc(), F.col("min_salary").desc())
)
