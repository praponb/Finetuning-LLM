from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Sia%") & F.col("supplier_name").like("%Import%"))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
    .orderBy("rating_bucket", "city_values", "city")
    .limit(10)
)
