from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("product_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("quantity", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("quantity_bucket").asc_nulls_first(), "id")
w_ordered = Window.orderBy("running_max_quantity_avg_6_prior", "id")
w7 = Window.orderBy("quantity_bucket", "id").rowsBetween(-6, -1)
w8 = Window.orderBy("running_sum_quantity_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("running_avg_quantity_avg_6_prior").desc(), "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") > "2024-10-01")
    .withColumn("last_restock_label", F.date_format("last_restock", "MMM yyyy"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 245, "High").when(F.col("quantity") >= 120, "Medium").otherwise("Low"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("quantity").over(w2).alias("quantity_avg_6_prior"),
    )
)

with_windows_inventory = (
    with_windows
    .filter(F.col("seq_no") <= 7)
    .withColumn("running_sum_quantity_avg_6_prior", F.sum("quantity_avg_6_prior").over(w_grp))
    .withColumn("running_max_quantity_avg_6_prior", F.max("quantity_avg_6_prior").over(w_grp))
    .withColumn("running_min_quantity_avg_6_prior", F.min("quantity_avg_6_prior").over(w_grp))
    .withColumn("running_avg_quantity_avg_6_prior", F.avg("quantity_avg_6_prior").over(w_grp))
    .withColumn("quantity_bucket", F.ntile(3).over(w_recent))
)

latest = (
    with_windows_inventory
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "quantity",
        "quantity_tier",
        "running_sum_quantity_avg_6_prior",
        "running_max_quantity_avg_6_prior",
        "running_min_quantity_avg_6_prior",
        "running_avg_quantity_avg_6_prior",
        "quantity_bucket",
    )
)

with_windows_inventory_2 = (
    latest
    .withColumn("next_running_avg_quantity_avg_6_prior", F.lead("running_avg_quantity_avg_6_prior", 1).over(w_ordered))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w7))
    .withColumn("first_known_running_avg_quantity_avg_6_prior", F.first("running_avg_quantity_avg_6_prior", ignorenulls=True).over(w8))
    .withColumn("prev_running_sum_quantity_avg_6_prior", F.lag("running_sum_quantity_avg_6_prior", 2).over(w_ordered))
    .withColumn("running_avg_quantity_avg_6_prior_bucket", F.ntile(4).over(w_hist))
)

result = (
    with_windows_inventory_2
    .cube(
        F.when(F.col("running_max_quantity_avg_6_prior") >= 239, "PREMIUM").otherwise("STANDARD").alias("running_max_quantity_avg_6_prior_tier"),
        F.when(F.col("first_known_running_avg_quantity_avg_6_prior") >= 253, "HIGH").otherwise("LOW").alias("first_known_running_avg_quantity_avg_6_prior_tier"),
    )
    .agg(
        F.percentile_approx("running_avg_quantity_avg_6_prior_bucket", [0.25, 0.5, 0.75]).alias("running_avg_quantity_avg_6_prior_bucket_quartiles"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("total_quantity") > 0)
)
