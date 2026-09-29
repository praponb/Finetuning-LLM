from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(((F.col("country") == "TH") & F.col("supplier_name").isNotNull()) | (F.upper(F.trim("supplier_name")) == "GLOBAL SUPPLY"))
)

summary = (
    src
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = summary.select("rating_bucket", "bangkok_count", "country_list")
