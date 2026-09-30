from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("order_date", "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-04-30 12:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") > 1570)
    .select(
        "s.id",
        "s.order_id",
        "s.status_length",
        F.col("o.order_date"),
        F.col("o.amount").alias("order_amount"),
    )
)

enriched = (
    joined
    .withColumn("last7_min_status_length", F.min("status_length").over(w))
    .withColumn("last7_avg_status_length", F.avg("status_length").over(w))
    .withColumn("last7_max_status_length", F.max("status_length").over(w))
    .withColumn("last7_count_status_length", F.count("status_length").over(w))
    .withColumn("prev_order_amount", F.lag("order_amount", 1).over(w2))
)

enriched_payments = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.status_length",
        "a.order_amount",
        "a.last7_min_status_length",
        "a.last7_max_status_length",
        "a.last7_count_status_length",
        "a.prev_order_amount",
        F.col("b.status"),
        F.col("b.channel"),
    )
)

result = (
    enriched_payments
    .groupBy(
        F.when(F.col("order_amount") >= 1220, "PREMIUM").otherwise("STANDARD").alias("order_amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.sum("last7_count_status_length") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("last7_count_status_length_per_shipped"),
        F.sum(F.when(F.col("channel") == "partner", F.col("status_length")).otherwise(0)).alias("partner_status_length"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
)
