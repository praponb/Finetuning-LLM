from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy(F.col("finance_count").desc(), "dept_name")
)
