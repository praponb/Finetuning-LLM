from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("price").asc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("store_id").orderBy(F.col("cost").desc(), "id")
w4 = Window.orderBy("cost", "id")
w5 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("row_seq", "id")
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("adjusted_row_seq_times_cost_dense_rank", "id")
w9 = Window.orderBy("price_percent_rank", "id")

joined = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isNull())
    .select("s.id", "s.store_id", "s.product_id", F.col("p.price"), F.col("p.cost"))
)

enriched = (
    joined
    .select(
        "id",
        "cost",
        F.percent_rank().over(w).alias("price_percent_rank"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

scored = (
    enriched
    .withColumn("last_known_cost_dense_rank", F.last("cost_dense_rank", ignorenulls=True).over(w5))
    .withColumn("next_price_percent_rank", F.lead("price_percent_rank", 1).over(w6))
    .withColumn("cumulative_cost", F.sum("cost").over(w7))
)

filtered = (
    scored
    .filter(F.col("cost") >= 220)
    .withColumn("row_seq_times_cost_dense_rank", (F.col("row_seq") * F.col("cost_dense_rank")))
)

filtered_inventory = (
    filtered
    .filter(F.col("last_known_cost_dense_rank") >= 4)
    .withColumn("adjusted_row_seq_times_cost_dense_rank", F.round((F.col("row_seq_times_cost_dense_rank") * 0.85), 2))
)

with_windows = (
    filtered_inventory
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w5))
    .withColumn("event_seq_2", F.row_number().over(w8))
    .withColumn("next_row_seq_times_cost_dense_rank", F.lead("row_seq_times_cost_dense_rank", 1).over(w9))
)

stats = (
    with_windows
    .groupBy(
        F.when(F.col("adjusted_row_seq_times_cost_dense_rank") >= 89, "LARGE").otherwise("SMALL").alias("adjusted_row_seq_times_cost_dense_rank_tier"),
        F.when(F.col("last_known_cost_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("last_known_cost_dense_rank_tier"),
        F.when(F.col("cost") >= 560, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.round(F.avg("event_seq_2"), 2).alias("avg_event_seq_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    stats
    .select(
        "adjusted_row_seq_times_cost_dense_rank_tier",
        "last_known_cost_dense_rank_tier",
        "cost_tier",
        "distinct_ids",
    )
    .orderBy(
        F.col("adjusted_row_seq_times_cost_dense_rank_tier").desc(),
        "distinct_ids",
    )
)
