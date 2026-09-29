from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
    )
)
