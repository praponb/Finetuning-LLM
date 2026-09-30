from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .withColumn("city_supplier_name_key", F.concat_ws("-", "city", "supplier_name"))
)

summary = (
    base
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"))
)

result = summary.select("city", "rating_bucket")
