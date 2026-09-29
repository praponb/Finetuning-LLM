from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
    )
    .filter(F.col("tokyo_budget") > 41)
    .select("location", "tokyo_budget", "sales_budget")
    .orderBy(F.col("sales_budget").desc(), "location")
)
