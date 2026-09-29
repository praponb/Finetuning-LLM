from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.0) & (F.col("country") == "TH"))
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("min_rating", "rating_bucket")
)
