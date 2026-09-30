from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity_whole", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity_whole").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("event_seq").asc_nulls_first())
w4 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("unit_price_plus_quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("next_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("quantity").isNull())
    .select("id", "order_id", "unit_price", F.floor("quantity").alias("quantity_whole"))
)

filtered = (
    base
    .filter(F.col("unit_price") < 360)
    .withColumn("quantity_whole_adjusted", F.round((F.col("quantity_whole") * 0.85), 2))
    .withColumn("unit_price_plus_quantity_whole", (F.col("unit_price") + F.col("quantity_whole")))
)

enriched = (
    filtered
    .filter(F.col("quantity_whole_adjusted").between(33, 79))
    .withColumn("next_unit_price", F.lead("unit_price").over(w))
    .withColumn("prev_quantity_whole_adjusted", F.lag("quantity_whole_adjusted").over(w))
    .withColumn("next_quantity_whole_adjusted", F.lead("quantity_whole_adjusted").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("quantity_whole_dense_rank", F.dense_rank().over(w2))
)

with_windows = (
    enriched
    .select(
        "id",
        "quantity_whole_adjusted",
        "unit_price_plus_quantity_whole",
        "next_unit_price",
        "prev_quantity_whole_adjusted",
        "next_quantity_whole_adjusted",
        F.cume_dist().over(w3).alias("event_seq_cume_dist"),
        F.avg("unit_price").over(w4).alias("unit_price_avg_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "quantity_whole_adjusted",
        "next_unit_price",
        "prev_quantity_whole_adjusted",
        "event_seq_cume_dist",
        "unit_price_avg_2",
        F.sum("next_unit_price").over(w5).alias("cumulative_next_unit_price"),
        F.sum("next_unit_price").over(w6).alias("running_next_unit_price"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("next_unit_price") >= 440, "PREMIUM").otherwise("STANDARD").alias("next_unit_price_tier"),
        F.when(F.col("event_seq_cume_dist") >= 0.66, "PREMIUM").otherwise("STANDARD").alias("event_seq_cume_dist_tier"),
        F.when(F.col("quantity_whole_adjusted") >= 76, "HIGH").otherwise("LOW").alias("quantity_whole_adjusted_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("event_seq_cume_dist", [0.25, 0.5, 0.75]).alias("event_seq_cume_dist_quartiles"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
