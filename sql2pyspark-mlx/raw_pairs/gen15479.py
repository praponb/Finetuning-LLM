from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Chiang Mai"]) & ((F.col("rating") <= 2.8) | (F.col("rating") < 3.8)))
    .withColumn("supplier_name_normalized", F.upper(F.trim("supplier_name")))
)

summary = (
    src
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.count("*").alias("row_count"),
    )
)

result = summary.select("city", "row_count")
