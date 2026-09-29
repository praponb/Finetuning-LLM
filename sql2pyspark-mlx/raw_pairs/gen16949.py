from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("quantity_adjusted", "id")
w5 = Window.orderBy("quantity_adjusted", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("quantity_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("last4_min_quantity_adjusted", "id")
w_grp = Window.orderBy(F.col("quantity_adjusted").desc(), "id")
w9 = Window.orderBy("running_running_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "quantity")
)

base = (
    latest
    .filter(F.col("quantity").isNotNull())
    .select("*", F.round((F.col("quantity") * 1.15), 2).alias("quantity_adjusted"))
)

enriched = (
    base
    .withColumn("running_quantity", F.sum("quantity").over(w2))
    .withColumn("quantity_adjusted_avg_3", F.avg("quantity_adjusted").over(w3))
    .withColumn("next_quantity_adjusted", F.lead("quantity_adjusted", 1).over(w4))
)

metrics = (
    enriched
    .withColumn("last4_min_quantity_adjusted", F.min("quantity_adjusted").over(w5))
    .withColumn("last4_sum_quantity_adjusted", F.sum("quantity_adjusted").over(w5))
    .withColumn("last4_count_quantity_adjusted", F.count("quantity_adjusted").over(w5))
    .withColumn("last4_max_quantity_adjusted", F.max("quantity_adjusted").over(w5))
    .withColumn("running_running_quantity", F.sum("running_quantity").over(w6))
)

scored = (
    metrics
    .filter(F.col("next_quantity_adjusted") <= 38)
    .select(
        "id",
        "quantity",
        "quantity_adjusted_avg_3",
        "running_running_quantity",
        F.lead("next_quantity_adjusted").over(w_ordered).alias("next_next_quantity_adjusted"),
        F.lag("running_running_quantity").over(w_ordered).alias("prev_running_running_quantity"),
        F.lead("running_running_quantity").over(w_ordered).alias("next_running_running_quantity"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w_grp).alias("seq_no_2"),
    )
)

scored_inventory = (
    scored
    .select(
        "*",
        F.last("seq_no", ignorenulls=True).over(w9).alias("last_known_seq_no"),
        F.sum("next_running_running_quantity").over(w2).alias("running_next_running_running_quantity"),
    )
)

base_inventory = (
    scored_inventory
    .filter(F.col("quantity") >= 145)
    .select(
        "id",
        "quantity",
        "quantity_adjusted_avg_3",
        "next_next_quantity_adjusted",
        "prev_running_running_quantity",
        "next_running_running_quantity",
        "seq_no",
        "seq_no_2",
        "last_known_seq_no",
        (F.col("quantity_adjusted_avg_3") * F.col("last_known_seq_no")).alias("quantity_adjusted_avg_3_times_last_known_seq_no"),
    )
)

result = (
    base_inventory
    .groupBy(
        F.when(F.col("next_next_quantity_adjusted") >= 76, "PREMIUM").otherwise("STANDARD").alias("next_next_quantity_adjusted_tier"),
    )
    .agg(
        F.percentile_approx("quantity_adjusted_avg_3", 0.95).alias("quantity_adjusted_avg_3_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
        F.round(F.avg("seq_no_2"), 2).alias("avg_seq_no_2"),
    )
)
