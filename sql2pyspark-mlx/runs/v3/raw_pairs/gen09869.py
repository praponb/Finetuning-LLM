from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Chiang Mai", "New York"]))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
