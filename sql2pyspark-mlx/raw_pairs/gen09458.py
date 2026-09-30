from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())
w2 = Window.orderBy("quantity_scaled", "id")
w3 = Window.partitionBy("store_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("quantity_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("dedup_rank", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("quantity_scaled").desc_nulls_last())
w_ordered = Window.orderBy("next_quantity_scaled_rounded", "id")
w_ordered_2 = Window.partitionBy("store_id").orderBy(F.col("next_quantity_scaled_rounded").asc_nulls_first())
w9 = Window.partitionBy("store_id").orderBy("dedup_rank", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy("next_quantity_scaled_rounded", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .filter(F.col("product_id") == 6)
    .withColumn("quantity_scaled_bucket", F.ntile(3).over(w2))
    .withColumn("running_quantity_scaled", F.sum("quantity_scaled").over(w3))
    .withColumn("running_quantity_scaled_2", F.sum("quantity_scaled").over(w4))
    .withColumn("next_quantity_scaled", F.lead("quantity_scaled", 1).over(w5))
)

cleaned = (
    scored
    .filter(F.col("running_quantity_scaled_2") == 169)
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity_scaled",
        "dedup_rank",
        "quantity_scaled_bucket",
        "running_quantity_scaled",
        "running_quantity_scaled_2",
        "next_quantity_scaled",
        F.round("next_quantity_scaled", 0).alias("next_quantity_scaled_rounded"),
    )
)

ranked_inventory = (
    cleaned
    .withColumn("row_num", F.rank().over(w6))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "store_id",
        "quantity_scaled",
        "dedup_rank",
        "running_quantity_scaled",
        "running_quantity_scaled_2",
        "next_quantity_scaled_rounded",
    )
)

scored_inventory = (
    ranked_inventory
    .select(
        "id",
        "running_quantity_scaled",
        "next_quantity_scaled_rounded",
        F.row_number().over(w5).alias("event_seq"),
        F.ntile(10).over(w_ordered).alias("next_quantity_scaled_rounded_bucket"),
        F.percent_rank().over(w_ordered_2).alias("next_quantity_scaled_rounded_percent_rank"),
        F.avg("next_quantity_scaled_rounded").over(w9).alias("next_quantity_scaled_rounded_avg_5"),
        F.avg("next_quantity_scaled_rounded").over(w10).alias("next_quantity_scaled_rounded_avg_6"),
    )
)

result = (
    scored_inventory
    .groupBy(
        F.when(F.col("running_quantity_scaled") >= 306, "PREMIUM").otherwise("STANDARD").alias("running_quantity_scaled_tier"),
    )
    .agg(
        F.percentile_approx("next_quantity_scaled_rounded_avg_6", [0.25, 0.5, 0.75]).alias("next_quantity_scaled_rounded_avg_6_quartiles"),
        F.percentile_approx("next_quantity_scaled_rounded_avg_6", [0.25, 0.5, 0.75]).alias("next_quantity_scaled_rounded_avg_6_quartiles_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
