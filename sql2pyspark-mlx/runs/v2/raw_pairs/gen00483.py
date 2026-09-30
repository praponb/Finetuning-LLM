from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .groupBy("dept_name")
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") >= 2277000)
    .select("dept_name", "max_budget")
    .orderBy(F.col("dept_name").desc(), F.col("max_budget").desc())
    .limit(1)
)
