from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.col("rating") > 3.2) & F.col("supplier_name").like("G%"))
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
)

summary = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.count("rating").alias("rating_count"),
    )
)

result = summary.orderBy("rating_count")
