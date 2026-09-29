from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("order_amount").asc_nulls_first(), "id")

enriched = (
    df_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.amount").alias("order_amount"),
        F.col("p.status").alias("order_status"),
        F.col("p.discount"),
    )
)

enriched_payments = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.channel"),
        F.col("p.shipping_fee"),
        F.col("p.discount").alias("order_discount"),
    )
)

deduped = (
    enriched_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("amount") >= 1390, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        "order_id",
        "order_status",
    )
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.percentile_approx("order_discount", [0.25, 0.5, 0.75]).alias("order_discount_quartiles"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
