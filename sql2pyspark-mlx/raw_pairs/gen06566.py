from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & (F.col("dept_name") == "Marketing") & (F.col("location") == "New York"))
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)
