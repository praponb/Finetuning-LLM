from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Tok%"))
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("total_count") > 9)
    .orderBy(F.col("total_count").desc())
)
