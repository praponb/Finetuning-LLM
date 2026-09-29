from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("amount", "id")
w_grp_2 = Window.partitionBy("status").orderBy("last3_count_quantity", "id")
w5 = Window.partitionBy("status").orderBy(F.col("quantity").desc())
w_hist = Window.partitionBy("status").orderBy("last3_max_quantity", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") == 5))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

metrics = (
    valid
    .withColumn("last3_count_quantity", F.count("quantity").over(w_recent))
    .withColumn("last3_min_quantity", F.min("quantity").over(w_recent))
    .withColumn("last3_sum_quantity", F.sum("quantity").over(w_recent))
    .withColumn("last3_max_quantity", F.max("quantity").over(w_recent))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w2))
)

joined = (
    metrics
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.quantity",
        "a.last3_count_quantity",
        "a.last3_min_quantity",
        "a.last3_max_quantity",
        "a.last_known_quantity",
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("o.amount"),
    )
)

with_windows = (
    joined
    .withColumn("amount_bucket", F.ntile(3).over(w_grp))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w_grp_2))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w5))
    .withColumn("last3_max_quantity_avg_5", F.avg("last3_max_quantity").over(w_hist))
)

stats = (
    with_windows
    .rollup(
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
    )
    .filter(F.col("distinct_order_ids") > 1)
)

result = (
    stats
    .select("quantity_tier", "pending_rows")
    .orderBy(F.col("quantity_tier").desc_nulls_last())
)
