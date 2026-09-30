from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel")
w2 = Window.orderBy("order_date", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2024-04-17").cast("date")) & F.col("store_id").isNotNull() & F.month("order_date").isin([5, 6, 11]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.floor("discount").alias("discount_whole"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

enriched = (
    filtered
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("event_seq", F.row_number().over(w2))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("year", "order_date").alias("year_start"),
        F.date_trunc("week", "order_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("amount_per_returned"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
    .orderBy(F.col("amount_per_returned").desc_nulls_first())
)
