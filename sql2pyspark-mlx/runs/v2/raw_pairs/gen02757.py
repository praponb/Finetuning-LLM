from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "New York"]) & (F.col("rating") >= 1.9) & (F.col("rating") <= 3.3))
    .withColumn("city_group", F.when(F.col("city").isin(["London", "Tokyo"]), "Tier 1").otherwise("Other"))
)

city_summary = (
    base
    .groupBy("city")
    .agg(F.count("*").alias("cnt"))
)

result = (
    city_summary
    .filter(F.col("cnt") > 8)
    .orderBy(F.col("cnt").desc(), "city")
    .limit(5)
)
