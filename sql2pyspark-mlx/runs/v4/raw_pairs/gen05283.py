from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "London"]) & F.col("location").isin(["Bangkok", "Berlin"]))
    .select("budget", "location", "dept_name")
)
