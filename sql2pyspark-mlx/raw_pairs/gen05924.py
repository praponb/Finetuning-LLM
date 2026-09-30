from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Singapore"]) & (F.col("dept_name") == "Legal"))
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0)).alias("sales_count"),
    )
    .orderBy(F.col("total_budget").desc(), "dept_name")
    .limit(5)
)
