from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["SG", "JP"]))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.avg("rating").alias("avg_rating"),
    )
    .orderBy(F.col("city").desc())
)
