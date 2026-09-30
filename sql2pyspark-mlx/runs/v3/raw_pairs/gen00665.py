from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("supplier_name").isNull() | (F.col("country") != "JP"))

agg = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(((100 * F.sum(F.when(F.col("country") == "SG", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sg"),
    )
)

result = (
    agg
    .filter(F.col("pct_sg") >= 79)
    .select("rating_bucket", "city_values", "pct_sg")
    .orderBy(F.col("pct_sg").desc(), "rating_bucket")
    .limit(3)
)
