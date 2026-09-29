from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "TH")
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy(F.col("rating_bucket").desc(), "country_values")
)
