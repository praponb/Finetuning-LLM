from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Finance"]) | (F.col("budget") < 3735000))
)

summary = (
    valid_departments
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
    )
)

result = (
    summary
    .select("location", "budget_range", "sales_count")
    .orderBy(F.col("budget_range").desc())
)
