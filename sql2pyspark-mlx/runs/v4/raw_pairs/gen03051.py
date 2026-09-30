from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "London"]))
    .groupBy(
        "country",
        "city",
        F.when(F.col("city").isin(["New York", "Chiang Mai"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("max_rating") < 3.4)
)
