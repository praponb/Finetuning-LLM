from pyspark.sql import functions as F

avg_budget = df_departments.filter(F.col("location").isin(["Bangkok", "New York", "Chiang Mai"])).agg(F.avg("budget")).first()[0]

result = (
    df_departments
    .filter(F.col("budget") >= avg_budget)
    .select("id", "budget", "location")
    .orderBy("id", F.col("location").desc())
)
