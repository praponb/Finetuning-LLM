from pyspark.sql import functions as F

avg_budget = df_departments.filter(F.col("location") == "Bangkok").agg(F.avg("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") >= avg_budget)
    .select("id", "budget")
    .orderBy(F.col("budget").desc())
)
