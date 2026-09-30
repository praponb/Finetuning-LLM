from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity_whole", "id")
w3 = Window.orderBy(F.col("last_restock").desc(), "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_whole").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("quantity", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("seq_no").desc_nulls_first())
w7 = Window.partitionBy("store_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id")
w9 = Window.orderBy("store_id_max_quantity", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-10-23")))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM-dd"))
    .withColumn("quantity_whole", F.ceil("quantity"))
)

with_windows = (
    valid
    .select(
        "*",
        F.max("quantity").over(w).alias("store_id_max_quantity"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_windows_inventory = (
    deduped
    .withColumn("seq_no_rank", F.rank().over(w6))
    .withColumn("last_known_store_id_max_quantity", F.last("store_id_max_quantity", ignorenulls=True).over(w7))
    .withColumn("product_id_sum_quantity_whole", F.sum("quantity_whole").over(w8))
)

scored = (
    with_windows_inventory
    .select(
        "*",
        F.row_number().over(w9).alias("event_seq_3"),
        F.avg("quantity").over(w8).alias("product_id_avg_quantity"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.opened_date"), F.col("b.city"), F.col("b.region"))
)

totals = (
    enriched
    .groupBy(
        F.when(F.col("event_seq_2") >= 8, "LARGE").otherwise("SMALL").alias("event_seq_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("event_count"),
        F.percentile_approx("store_id_max_quantity", 0.95).alias("store_id_max_quantity_p95"),
        F.sum(F.when(F.col("region") == "West", F.col("quantity_whole")).otherwise(0)).alias("west_quantity_whole"),
    )
)

result = (
    totals
    .select("event_seq_2_tier", "event_count", "store_id_max_quantity_p95")
    .orderBy("store_id_max_quantity_p95")
)
