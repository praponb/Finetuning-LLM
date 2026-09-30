from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .groupBy(F.when(F.col("sqft") >= 12810, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
)
