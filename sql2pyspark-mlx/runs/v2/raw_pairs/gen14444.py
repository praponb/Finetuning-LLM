from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai"]) & (F.col("budget") < 3592000))
    .select("id", "location")
)
