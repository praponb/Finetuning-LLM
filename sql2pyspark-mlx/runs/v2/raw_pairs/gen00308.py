from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Tokyo", "Singapore"]) & (F.col("budget") > 1984000))
    .select("budget")
)
