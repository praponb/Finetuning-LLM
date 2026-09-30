from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", "s.city", "s.rating", F.col("c.price"))
)

result = (
    latest_match
    .groupBy("city")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.avg(F.when(F.col("city") == "New York", F.col("rating"))).alias("new_york_avg_rating"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("price_per_london"),
    )
)
