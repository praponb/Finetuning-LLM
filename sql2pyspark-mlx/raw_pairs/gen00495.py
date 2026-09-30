from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isin(["Chiang Mai", "Berlin", "Tokyo"]) & F.col("city").isin(["Berlin", "New York", "London"])) | (F.col("country") != "UK"))
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy("city")
)
