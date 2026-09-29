from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%ion%"))
    .groupBy(
        "city",
        F.when(F.col("city").isin(["Berlin", "New York"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") > 5)
    .drop("unique_countrys")
)
