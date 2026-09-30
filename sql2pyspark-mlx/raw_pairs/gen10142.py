from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("rating_tier").orderBy(F.col("num_cities").desc())

with_windows = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
        F.sum("rating").over(w_running).alias("running_rating_2"),
        F.sum("rating").over(w_running).alias("running_rating_3"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("rating") >= 4.1, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        F.when(F.col("running_rating_2") >= 17, "LARGE").otherwise("SMALL").alias("running_rating_2_tier"),
    )
    .agg(
        F.percentile_approx("running_rating", [0.25, 0.5, 0.75]).alias("running_rating_quartiles"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("running_rating_2")).otherwise(0)).alias("tokyo_running_rating_2"),
        F.percentile_approx("running_rating_3", 0.9).alias("running_rating_3_p90"),
    )
    .withColumn("num_cities_percent_rank", F.percent_rank().over(w3))
)
