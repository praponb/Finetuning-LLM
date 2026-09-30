from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id")
w2 = Window.partitionBy(F.date_trunc("MONTH", "last_restock")).orderBy(F.col("cost").desc_nulls_last())
w_hist_2 = Window.partitionBy(F.date_trunc("YEAR", "last_restock"), "product_id").orderBy("cost", "id")
w_ordered = Window.partitionBy("store_id").orderBy("quantity_num_2", "id").rowsBetween(-5, -1)

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-09-17")))
    .select(
        "*",
        F.col("quantity").cast("string").alias("quantity_num"),
        F.col("quantity").cast("double").alias("quantity_num_2"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"))
)

with_windows = (
    joined
    .filter(F.col("quantity") < 124)
    .withColumn("store_id_stddev_samp_quantity_num_2", F.stddev_samp("quantity_num_2").over(w_hist))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("event_seq", F.row_number().over(w_hist_2))
    .withColumn("cost_avg_5_prior", F.avg("cost").over(w_ordered))
)

enriched = (
    with_windows
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.quantity",
        "a.quantity_num",
        "a.quantity_num_2",
        "a.store_id_stddev_samp_quantity_num_2",
        F.col("d.city"),
        F.col("d.sqft"),
    )
)

result = (
    enriched
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["store_city", "region", "inventory_city"], ["store_city", "region"], ["store_city"], []],
        F.col("d.city").alias("store_city"),
        F.col("d.region").alias("region"),
        F.col("src.city").alias("inventory_city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.quantity_num_2")).alias("max_quantity_num_2"),
    )
)
