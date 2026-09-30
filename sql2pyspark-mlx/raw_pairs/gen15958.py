from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w_hist = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w_recent = Window.orderBy("next_sqft", "id")
w4 = Window.orderBy(F.col("sqft_bucket").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.date_trunc("WEEK", "last_restock")).orderBy("sqft_bucket", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("next_sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("sqft_bucket_bucket", "id")
w9 = Window.orderBy(F.col("inventory_quantity").asc_nulls_first())
w10 = Window.orderBy("inventory_quantity", "id")

metrics = (
    df_stores
    .select(
        "id",
        "store_name",
        F.lead("sqft", 2).over(w).alias("next_sqft"),
        F.ntile(3).over(w_hist).alias("sqft_bucket"),
    )
)

with_windows = (
    metrics
    .withColumn("next_sqft_bucket", F.ntile(3).over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
    .withColumn("sqft_bucket_bucket", F.ntile(10).over(w4))
)

filtered = (
    with_windows
    .filter(F.col("sqft_bucket_bucket") == 6)
    .select(
        "*",
        F.substring("store_name", 1, 2).alias("store_name_prefix"),
        F.round("next_sqft_bucket", 2).alias("next_sqft_bucket_rounded"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.channel") == "web"), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.status").isin(["SHIPPED", "PENDING", "COMPLETED"])), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

not_flagged_stores = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.amount") >= 780), "left_anti")
)

result = (
    not_flagged_stores
    .filter(F.col("sqft_bucket").isNull())
    .select(
        "id",
        "store_name",
        "next_sqft",
        "sqft_bucket",
        "store_name_prefix",
        "inventory_quantity",
        F.avg("event_seq").over(w6).alias("event_seq_avg_3"),
        F.sum("sqft_bucket_bucket").over(w7).alias("running_sqft_bucket_bucket"),
        F.lag("inventory_quantity", 1).over(w8).alias("prev_inventory_quantity"),
        F.percent_rank().over(w9).alias("inventory_quantity_percent_rank"),
        F.ntile(10).over(w10).alias("inventory_quantity_bucket"),
    )
)
