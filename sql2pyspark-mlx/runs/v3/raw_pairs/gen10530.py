from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "US", "TH"]) & F.col("supplier_name").isNull())
)

agg = (
    recent_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("total_count"),
    )
)

result = agg.select("city", "rating_bucket", "city_list", "total_count")
