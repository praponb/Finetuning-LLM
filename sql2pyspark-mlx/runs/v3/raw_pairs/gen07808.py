from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Chiang Mai", "Tokyo"]) & F.col("location").isin(["London", "Singapore"]) & (F.col("budget") >= 3716000))
    .select("id", "budget")
)
