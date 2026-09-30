from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("rating") > 3.2) & (F.col("city") == "Tokyo")) | (F.length("supplier_name") > 12))
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
    )
    .filter(F.col("singapore_rating") > 76)
    .drop("singapore_rating")
)
