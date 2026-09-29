from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "SG")
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)
