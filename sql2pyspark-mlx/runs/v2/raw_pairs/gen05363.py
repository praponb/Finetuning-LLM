from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Berlin", "New York"]) & (F.col("budget") > 3211000))
    .select("id", "location", "budget")
)
