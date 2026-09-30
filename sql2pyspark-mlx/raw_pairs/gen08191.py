from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("last_restock", "id")
w_grp = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("next_quantity").desc(), "id")
w6 = Window.partitionBy("product_id", "store_id").orderBy("product_id_avg_prev_quantity", "id")
w7 = Window.partitionBy("store_id")
w8 = Window.partitionBy("store_id").orderBy("seq_no_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy(F.col("dedup_rank").desc(), "id")
w10 = Window.partitionBy("event_seq_tier").orderBy(F.col("distinct_ids").desc())

scored = (
    df_inventory
    .select(
        "*",
        F.row_number().over(w_part).alias("seq_no"),
        F.lag("quantity", 1).over(w_part).alias("prev_quantity"),
    )
)

metrics = (
    scored
    .filter(F.col("prev_quantity") >= 156)
    .withColumn("product_id_max_prev_quantity", F.max("prev_quantity").over(w_grp))
    .withColumn("product_id_avg_prev_quantity", F.avg("prev_quantity").over(w_grp))
    .withColumn("next_quantity", F.lead("quantity", 1, 0).over(w3))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w4))
)

metrics_inventory = (
    metrics
    .select(
        "id",
        "store_id",
        "last_restock",
        "next_quantity",
        F.row_number().over(w5).alias("seq_no_2"),
        F.lag("product_id_avg_prev_quantity", 2, 0).over(w6).alias("prev_product_id_avg_prev_quantity"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w7)), 4).alias("seq_no_share"),
    )
)

deduped = (
    metrics_inventory
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.opened_date"))
)

with_windows = (
    enriched
    .select(
        "id",
        "opened_date",
        F.sum("seq_no_2").over(w8).alias("running_sum_seq_no_2"),
        F.count("seq_no_2").over(w8).alias("running_count_seq_no_2"),
        F.avg("seq_no_2").over(w8).alias("running_avg_seq_no_2"),
        F.max("seq_no_2").over(w8).alias("running_max_seq_no_2"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 7, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("event_seq", 0.95).alias("event_seq_p95"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles_2"),
    )
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w10))
)
