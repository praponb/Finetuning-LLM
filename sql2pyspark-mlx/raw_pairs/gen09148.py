from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter((F.col("country") == "TH") | F.col("city").isin(["Chiang Mai", "Tokyo", "Bangkok"]))
)

summary = (
    src
    .groupBy("city")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = (
    summary
    .filter(F.col("avg_rating") >= 34)
    .orderBy("avg_rating", "city")
    .limit(10)
)
