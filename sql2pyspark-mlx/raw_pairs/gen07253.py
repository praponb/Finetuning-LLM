from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("quantity").desc(), "id")
w_recent = Window.partitionBy("product_id", "store_id").orderBy("quantity_bucket_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.date_trunc("WEEK", "last_restock")).orderBy(F.col("last4_min_quantity").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("quantity_bucket").asc_nulls_first(), "id")
w6 = Window.partitionBy("store_id").orderBy("last4_min_quantity_minus_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("last4_min_quantity").desc(), "id")
w8 = Window.partitionBy("store_id").orderBy("latest_rank", "id").rowsBetween(-2, Window.currentRow)

metrics = (
    df_inventory
    .filter(F.col("last_restock").between("2024-06-01", "2024-09-30"))
    .withColumn("last4_sum_quantity", F.sum("quantity").over(w_grp))
    .withColumn("last4_avg_quantity", F.avg("quantity").over(w_grp))
    .withColumn("last4_min_quantity", F.min("quantity").over(w_grp))
    .withColumn("quantity_bucket", F.ntile(10).over(w2))
)

cleaned = (
    metrics
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-04-17")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last4_sum_quantity",
        "last4_min_quantity",
        "quantity_bucket",
        F.floor("quantity_bucket").alias("quantity_bucket_whole"),
    )
)

metrics_inventory = (
    cleaned
    .select(
        "id",
        "store_id",
        "last4_min_quantity",
        "quantity_bucket",
        "quantity_bucket_whole",
        F.sum("quantity_bucket_whole").over(w_recent).alias("running_sum_quantity_bucket_whole"),
        F.count("quantity_bucket_whole").over(w_recent).alias("running_count_quantity_bucket_whole"),
        F.avg("quantity_bucket_whole").over(w_recent).alias("running_avg_quantity_bucket_whole"),
        F.ntile(4).over(w_hist).alias("last4_min_quantity_bucket"),
    )
)

ranked = (
    metrics_inventory
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("last4_min_quantity") > 190)
    .select(
        "*",
        (F.col("last4_min_quantity") - F.col("quantity_bucket")).alias("last4_min_quantity_minus_quantity_bucket"),
        F.round((F.col("last4_min_quantity_bucket") * 0.85), 2).alias("adjusted_last4_min_quantity_bucket"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.count("running_sum_quantity_bucket_whole").over(w6).alias("running_count_running_sum_quantity_bucket_whole"),
        F.sum("running_sum_quantity_bucket_whole").over(w6).alias("running_sum_running_sum_quantity_bucket_whole"),
        F.min("running_sum_quantity_bucket_whole").over(w6).alias("running_min_running_sum_quantity_bucket_whole"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

metrics_inventory_2 = (
    with_windows
    .select(
        "id",
        "store_id",
        "last4_min_quantity",
        "quantity_bucket",
        "last4_min_quantity_minus_quantity_bucket",
        "running_count_running_sum_quantity_bucket_whole",
        "running_sum_running_sum_quantity_bucket_whole",
        F.avg("quantity_bucket_whole").over(w8).alias("last3_avg_quantity_bucket_whole"),
        F.max("quantity_bucket_whole").over(w8).alias("last3_max_quantity_bucket_whole"),
        F.sum("quantity_bucket_whole").over(w8).alias("last3_sum_quantity_bucket_whole"),
        F.count("quantity_bucket_whole").over(w8).alias("last3_count_quantity_bucket_whole"),
    )
)

grouped = (
    metrics_inventory_2
    .groupBy(
        F.when(F.col("last3_avg_quantity_bucket_whole") >= 61, "LARGE").otherwise("SMALL").alias("last3_avg_quantity_bucket_whole_tier"),
        "store_id",
    )
    .agg(
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
)

result = (
    grouped
    .select(
        "last3_avg_quantity_bucket_whole_tier",
        "store_id",
        "avg_quantity_bucket",
        "distinct_store_ids",
    )
)
