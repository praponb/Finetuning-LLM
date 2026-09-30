from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w4 = Window.orderBy(F.col("prev_quantity").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("first_known_quantity", "id")
w_grp = Window.orderBy("prev_quantity", "id")
w7 = Window.partitionBy("product_id")
w8 = Window.partitionBy("product_id").orderBy("prev_quantity_bucket_2", "id")
w9 = Window.partitionBy("product_id").orderBy("prev_quantity", "id")
w_part = Window.partitionBy("product_id").orderBy("first_known_quantity_bucket", "id")
w_recent = Window.orderBy("prev_prev_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("prev_first_known_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("prev_quantity_2").desc(), "id")

enriched = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched_inventory = (
    latest
    .select(
        "id",
        "product_id",
        "quantity",
        "prev_quantity",
        "first_known_quantity",
        F.ntile(4).over(w4).alias("prev_quantity_bucket"),
        F.ntile(3).over(w5).alias("first_known_quantity_bucket"),
        F.ntile(3).over(w_grp).alias("prev_quantity_bucket_2"),
    )
)

with_windows = (
    enriched_inventory
    .filter(F.col("prev_quantity") != 228)
    .select(
        "id",
        "first_known_quantity",
        "prev_quantity_bucket",
        "first_known_quantity_bucket",
        "prev_quantity_bucket_2",
        F.max("prev_quantity").over(w7).alias("product_id_max_prev_quantity"),
        F.ntile(10).over(w8).alias("prev_quantity_bucket_2_bucket"),
        F.lag("first_known_quantity_bucket", 1).over(w5).alias("prev_first_known_quantity_bucket"),
        F.lag("quantity", 1).over(w9).alias("prev_quantity_2"),
        F.lag("prev_quantity_bucket", 2).over(w_part).alias("prev_prev_quantity_bucket"),
    )
)

base = (
    with_windows
    .filter(F.col("first_known_quantity").between(76, 229))
    .select(
        "id",
        "first_known_quantity",
        "prev_quantity_bucket",
        "prev_quantity_bucket_2_bucket",
        "prev_first_known_quantity_bucket",
        "prev_quantity_2",
        "prev_prev_quantity_bucket",
        (F.col("prev_quantity_bucket") + F.col("prev_first_known_quantity_bucket")).alias("prev_quantity_bucket_plus_prev_first_known_quantity_bucket"),
    )
)

enriched_inventory_2 = (
    base
    .select(
        "*",
        F.sum("prev_first_known_quantity_bucket").over(w_recent).alias("cumulative_prev_first_known_quantity_bucket"),
        F.sum("prev_first_known_quantity_bucket").over(w12).alias("running_prev_first_known_quantity_bucket"),
        F.ntile(10).over(w13).alias("prev_quantity_2_bucket"),
    )
)

summary = (
    enriched_inventory_2
    .groupBy(
        F.when(F.col("prev_prev_quantity_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("prev_prev_quantity_bucket_tier"),
        F.when(F.col("cumulative_prev_first_known_quantity_bucket") >= 41, "PREMIUM").otherwise("STANDARD").alias("cumulative_prev_first_known_quantity_bucket_tier"),
    )
    .agg(
        F.round(F.avg("running_prev_first_known_quantity_bucket"), 2).alias("avg_running_prev_first_known_quantity_bucket"),
        F.count("*").alias("event_count"),
        F.sum("cumulative_prev_first_known_quantity_bucket").alias("total_cumulative_prev_first_known_quantity_bucket"),
        F.sum("prev_prev_quantity_bucket").alias("total_prev_prev_quantity_bucket"),
        F.sum("prev_first_known_quantity_bucket").alias("total_prev_first_known_quantity_bucket"),
    )
)

result = (
    summary
    .select(
        "prev_prev_quantity_bucket_tier",
        "cumulative_prev_first_known_quantity_bucket_tier",
        "avg_running_prev_first_known_quantity_bucket",
        "event_count",
        "total_prev_prev_quantity_bucket",
    )
)
