from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("country_lower", F.lower("country"))
)

suppliers_stats = (
    recent_suppliers
    .groupBy("city")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)

result = (
    suppliers_stats
    .filter(F.col("stddev_rating") < 61)
    .select("city", "stddev_rating")
    .orderBy(F.col("stddev_rating").desc())
)
