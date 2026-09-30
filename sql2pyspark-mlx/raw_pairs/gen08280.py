from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("product_id").orderBy("quantity", "id")
w_ordered = Window.partitionBy("product_id").orderBy("last7_count_quantity", "id")
w_grp = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.orderBy("prev_quantity_bucket", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last7_count_quantity", "id").rowsBetween(-6, -1)
w8 = Window.orderBy("quantity_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last7_min_quantity_avg_6_whole_avg_6_prior", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.orderBy("last7_min_quantity_avg_6_whole_avg_6_prior", "id")
w11 = Window.partitionBy("last7_min_quantity_avg_6_whole_tier").orderBy(F.col("quantity_bit_or").desc())

with_windows = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.min("quantity").over(w_part).alias("last7_min_quantity"),
        F.max("quantity").over(w_part).alias("last7_max_quantity"),
        F.count("quantity").over(w_part).alias("last7_count_quantity"),
        F.ntile(3).over(w_hist).alias("quantity_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "quantity",
        "last7_max_quantity",
        "last7_count_quantity",
        "quantity_bucket",
        F.lag("quantity_bucket", 1, 0).over(w_ordered).alias("prev_quantity_bucket"),
        F.avg("quantity").over(w_grp).alias("quantity_avg_6"),
    )
)

with_windows_inventory = (
    scored
    .withColumn("last7_min_quantity_avg_6", F.min("quantity_avg_6").over(w_recent))
    .withColumn("last7_sum_quantity_avg_6", F.sum("quantity_avg_6").over(w_recent))
    .withColumn("last7_max_quantity_avg_6", F.max("quantity_avg_6").over(w_recent))
    .withColumn("last7_count_quantity_avg_6", F.count("quantity_avg_6").over(w_recent))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w6))
)

base = (
    with_windows_inventory
    .filter(F.col("quantity") == 94)
    .select(
        "id",
        "quantity",
        "last7_max_quantity",
        "last7_count_quantity",
        "prev_quantity_bucket",
        "quantity_avg_6",
        "last7_sum_quantity_avg_6",
        "last7_max_quantity_avg_6",
        "last7_count_quantity_avg_6",
        "cumulative_quantity",
        F.floor("last7_min_quantity_avg_6").alias("last7_min_quantity_avg_6_whole"),
    )
)

with_windows_inventory_2 = (
    base
    .select(
        "id",
        "quantity",
        "last7_min_quantity_avg_6_whole",
        F.avg("last7_min_quantity_avg_6_whole").over(w7).alias("last7_min_quantity_avg_6_whole_avg_6_prior"),
        F.sum("last7_max_quantity").over(w8).alias("running_last7_max_quantity"),
    )
)

enriched = (
    with_windows_inventory_2
    .select(
        "*",
        F.avg("last7_min_quantity_avg_6_whole_avg_6_prior").over(w9).alias("last7_min_quantity_avg_6_whole_avg_6_prior_avg_6"),
        F.ntile(10).over(w10).alias("last7_min_quantity_avg_6_whole_avg_6_prior_bucket"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") < 133)
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("last7_min_quantity_avg_6_whole") >= 76, "LARGE").otherwise("SMALL").alias("last7_min_quantity_avg_6_whole_tier"),
        F.when(F.col("quantity") >= 188, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("last7_min_quantity_avg_6_whole_avg_6_prior_avg_6", 0.95).alias("last7_min_quantity_avg_6_whole_avg_6_prior_avg_6_p95"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .filter(F.col("last7_min_quantity_avg_6_whole_avg_6_prior_avg_6_p95") > 0)
    .withColumn("quantity_bit_or_dense_rank", F.dense_rank().over(w11))
)
