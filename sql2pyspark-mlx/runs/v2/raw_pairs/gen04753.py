from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location").isin(["Singapore", "Berlin"]) & (F.col("budget") <= 1235000)) | F.col("budget").isNotNull())
    .select("budget", "location")
    .orderBy(F.col("budget").desc(), "location")
)
