from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & (F.col("country") != "UK"))
    .groupBy(
        F.when(F.col("city").isin(["Chiang Mai", "Singapore"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.min("rating").alias("min_rating"),
    )
)
