from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .groupBy("location")
    .agg(F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"))
    .orderBy(F.col("location").desc())
)
