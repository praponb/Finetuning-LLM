from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    latest_match
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 16882, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.count_if(F.col("region") == "East").alias("east_events"),
        F.percentile_approx("inventory_quantity", [0.25, 0.5, 0.75]).alias("inventory_quantity_quartiles"),
        F.avg(F.when(F.col("city") == "Berlin", F.col("sqft"))).alias("berlin_avg_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.max(F.when(F.col("city") == "Singapore", F.col("inventory_quantity"))).alias("singapore_max_inventory_quantity"),
    )
    .filter(F.col("singapore_max_inventory_quantity") > 0)
    .orderBy(F.col("east_events").desc())
)
