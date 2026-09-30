from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.col("city").isin(["Tokyo", "Singapore", "Berlin"]) & F.col("country").isin(["US", "TH", "SG"])) | (F.col("city") == "Chiang Mai"))
)

summary = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = (
    summary
    .select("rating_bucket", "city", "city_values")
    .orderBy(F.col("city_values").desc(), "city", "rating_bucket")
    .limit(3)
)
