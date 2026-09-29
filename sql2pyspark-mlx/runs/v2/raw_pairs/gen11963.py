from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "HR", "Sales"]))
    .select("id", "budget", "dept_name", F.ceil("budget").alias("budget_whole"))
    .orderBy(F.col("budget_whole").desc())
)
