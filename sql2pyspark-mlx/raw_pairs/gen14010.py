from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id", "subscription_id")
w2 = Window.partitionBy("customer_id").orderBy("order_date_period", "id", "subscription_id")
w3 = Window.partitionBy("customer_id").orderBy("order_date_period", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-05-13"))) & F.col("customer_id").isin([14, 35]))
    .select("*", F.date_trunc("MONTH", "order_date").alias("order_date_period"))
)

matched = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.order_date_period") >= F.col("sub.start_date")) & (F.col("s.order_date_period") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

deduped = (
    matched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_gaps = (
    deduped
    .withColumn("new_group_flag", F.when(F.col("order_date_period") - F.lag("order_date_period").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_orders = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

joined = (
    sessions_orders
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.opened_date"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("amount") >= 1730, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("shipping_fee") >= 31, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.min("order_date_period").alias("started_at"),
        ((F.unix_timestamp(F.max("order_date_period")) - F.unix_timestamp(F.min("order_date_period"))) / 60).alias("duration_min"),
        F.min("order_date_period").alias("first_seen"),
        F.count_if(F.col("plan") == "premium").alias("premium_events"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("monthly_fee_per_mobile"),
        F.min("order_date_period").alias("first_order_date_period"),
    )
)
