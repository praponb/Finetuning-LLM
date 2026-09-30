from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("country").isin(["US", "JP"]) & (F.col("rating").between(2.2, 3.5) | F.col("supplier_name").like("%Toky%")))
    .withColumn("rating_band", F.when(F.col("rating") >= 3.6, "High").when(F.col("rating") >= 1.7, "Medium").otherwise("Low"))
)

agg = (
    src
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
)

result = (
    agg
    .filter(F.col("pct_singapore") < 63)
    .select("city", "pct_singapore")
)
