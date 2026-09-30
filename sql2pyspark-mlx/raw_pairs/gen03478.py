from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter((F.lower(F.trim("supplier_name")) == "global supply") & (F.col("city") == "Chiang Mai"))
    .withColumn("city_country_key", F.concat_ws(",", "city", "country"))
)

agg = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"))
)

result = (
    agg
    .filter(F.col("sg_count") < 3)
    .select("rating_bucket", "sg_count")
)
