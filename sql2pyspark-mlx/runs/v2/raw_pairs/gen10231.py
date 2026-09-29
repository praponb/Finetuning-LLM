from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("rating").isNotNull() & F.col("supplier_name").like("%Cit%"))
)

summary = (
    filtered_suppliers
    .groupBy("city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.count("rating").alias("rating_count"),
    )
)

result = (
    summary
    .select("city", "max_rating", "rating_count")
    .orderBy(F.col("rating_count").desc(), F.col("max_rating").desc())
)
