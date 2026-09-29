from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") & F.col("supplier_name").like("%Wholesal%"))
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("cnt", "rating_bucket", "country")
    .limit(5)
)
