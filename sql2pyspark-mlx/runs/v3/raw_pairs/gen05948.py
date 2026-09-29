from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") <= "2023-04-21") | F.col("dept_id").isin([6, 11, 13, 38]))
    .groupBy("is_remote")
    .agg(
        F.max("salary").alias("max_salary"),
        F.count("*").alias("row_count"),
    )
)
