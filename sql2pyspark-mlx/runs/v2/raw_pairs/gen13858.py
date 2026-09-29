from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("rating") <= 2.4)

summary = (
    src
    .groupBy("city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "JP", 1).otherwise(0))) / F.count("*")), 1).alias("pct_jp"),
    )
)

result = (
    summary
    .filter(F.col("min_rating") >= 3.5)
    .select("city", "min_rating", "pct_jp")
    .orderBy(F.col("pct_jp").desc())
)
