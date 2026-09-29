from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 2014000) | F.col("dept_name").isin(["Engineering", "Sales"]))
    .agg(
        F.max("budget").alias("max_budget"),
        F.count("*").alias("cnt"),
    )
)
