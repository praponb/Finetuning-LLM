from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("store_amount").desc())

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-04").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.floor("discount").alias("discount_whole"),
        F.substring("channel", 1, 2).alias("channel_prefix"),
    )
)

result = (
    valid
    .groupBy(
        "status",
        F.when(F.col("discount") >= 0.34, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
    )
    .withColumn("store_amount_rank", F.rank().over(w))
    .orderBy(F.col("discount_tier").desc())
)
