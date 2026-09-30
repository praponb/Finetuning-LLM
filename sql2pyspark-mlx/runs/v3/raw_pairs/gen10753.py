from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.1) & F.col("supplier_name").like("%Supp%"))
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.avg("rating").alias("avg_rating"),
    )
)
