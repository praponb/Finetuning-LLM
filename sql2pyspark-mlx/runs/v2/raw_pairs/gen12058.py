from pyspark.sql import functions as F

max_budget = df_departments.filter(F.col("budget") > 1444000).agg(F.max("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") > max_budget)
    .select("id", "budget", "location")
    .orderBy("location", "id")
    .limit(10)
)
