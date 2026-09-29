from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

ranked = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .groupBy("country")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("rating", 0.9).alias("rating_p90"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("rating_per_london"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating"))).alias("berlin_total_rating"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
    )
    .orderBy(F.col("num_cities_2").desc())
)
