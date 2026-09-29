from pyspark.sql import functions as F

base = df_suppliers.filter(~((F.col("country") != "SG") & (F.col("rating") < 2.9)))

summary = (
    base
    .groupBy("city")
    .agg(F.avg("rating").alias("avg_rating"))
)

result = (
    summary
    .filter(F.col("avg_rating") >= 3.1)
    .select("city", "avg_rating")
    .orderBy(F.col("avg_rating").desc())
)
