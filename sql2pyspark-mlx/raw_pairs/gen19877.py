from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Chiang Mai"]) & (F.col("budget") <= 3315000))
    .select("id", "location", F.floor("budget").alias("budget_whole"))
    .orderBy(F.col("id").desc(), F.col("location").desc())
)
