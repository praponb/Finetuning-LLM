from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Singapore", "New York"]))
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 5)
    .drop("cnt")
)
