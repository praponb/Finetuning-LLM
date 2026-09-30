from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 10)
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        F.when(F.col("city").isin(["Berlin", "London"]), "priority").otherwise("other").alias("city_group"),
        "country",
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy("city_group")
)
