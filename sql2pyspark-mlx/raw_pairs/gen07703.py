from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id").orderBy("order_date")
w2 = Window.partitionBy("customer_id").orderBy("amount_with_shipping", "id")
w3 = Window.orderBy("amount", "id").rowsBetween(-6, -1)

base = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & (F.col("amount") <= 590))
    .withColumn("amount_with_shipping", F.round((F.col("shipping_fee") / F.col("amount")), 2))
    .withColumn("channel_prefix", F.substring("channel", 1, 2))
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.channel",
        "src.amount",
        "src.discount",
        "src.amount_with_shipping",
        "src.channel_prefix",
        F.col("b.age"),
        F.col("b.city"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "SUCCESS"), "left_semi")
)

metrics = (
    eligible
    .select(
        "id",
        "channel",
        "amount",
        "discount",
        "channel_prefix",
        "row_num",
        F.lag("age").over(w2).alias("prev_age"),
        F.lead("age").over(w2).alias("next_age"),
        F.lag("amount_with_shipping").over(w2).alias("prev_amount_with_shipping"),
        F.row_number().over(w2).alias("event_seq"),
        F.avg("age").over(w3).alias("age_avg_6_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "amount",
        "discount",
        "channel_prefix",
        "row_num",
        "prev_age",
        "next_age",
        "prev_amount_with_shipping",
        "event_seq",
    )
)
