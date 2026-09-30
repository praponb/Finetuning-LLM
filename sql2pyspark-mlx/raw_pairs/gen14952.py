from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock"), "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "last_restock"), "store_id").orderBy("quantity")
w_recent = Window.orderBy("quantity", "id")
w_hist = Window.partitionBy("store_id")
w5 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity_cume_dist").desc_nulls_last())
w6 = Window.orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("quantity_cume_dist_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("running_count_store_id_max_quantity_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy("store_id_max_quantity_cume_dist", "id")

enriched = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 2, 8]))
    .select(
        "id",
        "store_id",
        "product_id",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
        F.lead("quantity", 2).over(w_recent).alias("next_quantity"),
    )
)

scored = (
    enriched
    .withColumn("store_id_max_quantity_cume_dist", F.max("quantity_cume_dist").over(w_hist))
    .withColumn("quantity_cume_dist_cume_dist", F.cume_dist().over(w5))
    .withColumn("running_next_quantity", F.sum("next_quantity").over(w6))
)

scored_inventory = (
    scored
    .select(
        "id",
        "store_id",
        "cumulative_quantity",
        "store_id_max_quantity_cume_dist",
        "quantity_cume_dist_cume_dist",
        F.count("store_id_max_quantity_cume_dist").over(w7).alias("running_count_store_id_max_quantity_cume_dist"),
        F.min("store_id_max_quantity_cume_dist").over(w7).alias("running_min_store_id_max_quantity_cume_dist"),
        F.sum("store_id_max_quantity_cume_dist").over(w7).alias("running_sum_store_id_max_quantity_cume_dist"),
    )
)

with_windows = (
    scored_inventory
    .select(
        "*",
        F.avg("cumulative_quantity").over(w8).alias("cumulative_quantity_avg_6"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.opened_date"), F.col("d.city"), F.col("d.region"))
)

result = (
    with_dims
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .withColumn("cumulative_quantity_tier", F.when(F.col("cumulative_quantity") >= 975, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "cumulative_quantity_tier"], ["quarter_start"], []],
        "quarter_start",
        "cumulative_quantity_tier",
    )
    .agg(
        F.round(F.sum("store_id_max_quantity_cume_dist") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("store_id_max_quantity_cume_dist_per_north"),
        F.percentile_approx("running_sum_store_id_max_quantity_cume_dist", [0.25, 0.5, 0.75]).alias("running_sum_store_id_max_quantity_cume_dist_quartiles"),
        F.min("running_min_store_id_max_quantity_cume_dist").alias("min_running_min_store_id_max_quantity_cume_dist"),
        F.round(F.avg("cumulative_quantity_avg_6"), 2).alias("avg_cumulative_quantity_avg_6"),
    )
)
