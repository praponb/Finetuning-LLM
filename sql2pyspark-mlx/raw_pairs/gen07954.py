from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Trade%"))
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
    .orderBy(F.col("city_values").desc())
)
