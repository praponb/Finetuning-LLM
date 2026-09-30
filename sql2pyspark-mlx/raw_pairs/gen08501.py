from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("customer_id").orderBy("next_amount", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.orderBy("amount", "id")
w4 = Window.partitionBy("customer_id", "city").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("prev_amount", "id")

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-07-02").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        F.upper("channel").alias("channel_upper"),
        F.when(F.col("discount") >= 0.32, "High").when(F.col("discount") >= 0.19, "Medium").otherwise("Low").alias("discount_tier"),
    )
)

cleaned = (
    filtered
    .filter(F.col("store_id") == 19)
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "channel_upper",
        "discount_tier",
        F.when(F.col("channel").isin(["web", "mobile"]), "Group A").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

metrics = (
    cleaned
    .withColumn("next_amount", F.lead("amount").over(w_part))
    .withColumn("prev_amount", F.lag("amount").over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
)

joined = (
    metrics
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.city"), F.col("d.store_name"))
)

enriched = (
    joined
    .filter(F.col("city").isin(["Singapore", "Berlin"]))
    .select(
        "id",
        "customer_id",
        "discount_tier",
        "channel_group",
        "resolved_channel",
        "prev_amount",
        "event_seq",
        "city",
        F.avg("next_amount").over(w2).alias("next_amount_avg_2"),
        F.lead("event_seq", 1).over(w_running).alias("next_event_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "resolved_channel",
        "prev_amount",
        "event_seq",
        "city",
        "next_event_seq",
        F.first("next_amount_avg_2", ignorenulls=True).over(w4).alias("first_known_next_amount_avg_2"),
        F.lag("next_amount_avg_2", 2).over(w5).alias("prev_next_amount_avg_2"),
    )
)
