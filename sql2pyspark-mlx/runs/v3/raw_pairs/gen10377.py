from pyspark.sql import functions as F

max_budget = df_departments.filter(F.col("dept_name") != "Legal").agg(F.max("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") >= max_budget)
    .select("id", "budget", "dept_name")
    .orderBy("dept_name", F.col("budget").desc(), "id")
    .limit(5)
)
