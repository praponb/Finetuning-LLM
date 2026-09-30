from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment", "status").orderBy("discount", "id", "subscription_id")

attributed = (
    df_orders
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

joined = (
    attributed
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.age") == 44)
    .select(
        "a.id",
        "a.customer_id",
        "a.store_id",
        "a.discount",
        "a.subscription_id",
        "a.plan",
        "a.monthly_fee",
        F.col("p.city"),
        F.col("p.segment"),
    )
)

latest_match = (
    joined
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.discount",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        "s.segment",
        F.col("c.paid_at").alias("payment_paid_at"),
        F.col("c.amount").alias("payment_amount"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < F.col("s.payment_paid_at")), "left_anti")
)

latest_match_orders = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.discount",
        "s.subscription_id",
        "s.segment",
        F.col("c.status"),
    )
)

deduped = (
    latest_match_orders
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "subscription_id", "segment", "status")
)

kept = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity") <= 4), "left_anti")
)

result = (
    kept
    .groupBy("subscription_id")
    .agg(
        F.round(F.sum("subscription_id") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("subscription_id_per_refunded"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("consumer_count") >= 1)
    .orderBy(F.col("subscription_id_per_refunded").asc_nulls_last())
)
