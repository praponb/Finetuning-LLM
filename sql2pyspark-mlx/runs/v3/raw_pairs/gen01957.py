from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2856000)
    .select("location")
    .unionByName(df_departments.filter(F.col("budget").isNotNull()).select("location"))
    .orderBy(F.col("location").desc())
)
