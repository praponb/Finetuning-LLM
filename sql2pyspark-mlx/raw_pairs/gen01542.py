from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount_times_discount").asc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy("amount_times_discount", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.month("order_date"), "store_id").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_orders
    .filter(F.col("shipping_fee").between(11, 24))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

cleaned = (
    filtered
    .filter((F.col("order_date") > F.lit("2023-08-25").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount_times_discount",
        F.trim("status").alias("clean_status"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .filter(F.col("amount_times_discount") < 39)
    .select(
        "id",
        "order_date",
        "status",
        "amount_times_discount",
        "clean_status",
        "dedup_rank",
        F.avg("amount_times_discount").over(w2).alias("last7_avg_amount_times_discount"),
        F.max("amount_times_discount").over(w2).alias("last7_max_amount_times_discount"),
        F.min("amount_times_discount").over(w2).alias("last7_min_amount_times_discount"),
        F.avg("amount_times_discount").over(w3).alias("amount_times_discount_avg_6"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_date",
        "status",
        "clean_status",
        "last7_avg_amount_times_discount",
        "last7_min_amount_times_discount",
        "amount_times_discount_avg_6",
    )
    .orderBy(F.col("amount_times_discount_avg_6").desc())
)
