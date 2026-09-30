from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("channel").orderBy(F.col("order_shipping_fee").asc_nulls_first())
w4 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id"), F.col("s.promo_id_2")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.age",
        F.col("c.status").alias("order_status"),
        F.col("c.channel").alias("order_channel"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.signup_date",
        "s.is_active",
        "s.age",
        "s.order_channel",
        F.col("c.channel"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.year(F.col("x.start_date")) == 2025), "left_semi")
)

metrics = (
    kept
    .filter(F.col("channel").isin(["mobile", "store", "partner"]))
    .select(
        "id",
        "signup_date",
        "is_active",
        "channel",
        "order_shipping_fee",
        F.cume_dist().over(w3).alias("order_shipping_fee_cume_dist"),
        F.first("channel", ignorenulls=True).over(w4).alias("first_known_channel"),
    )
)

attributed = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.signup_date") >= F.col("pr.starts_on")) & (F.col("s.signup_date") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

matched = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.signup_date") >= F.col("pr.starts_on")) & (F.col("e.signup_date") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_latest_customers = (
    matched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country"))
)

result = (
    with_latest_customers
    .select(
        "id",
        "signup_date",
        "is_active",
        "channel",
        "order_shipping_fee",
        "first_known_channel",
        "promo_id",
        "promo_code",
        "promo_id_2",
        F.when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "GROUP_B").otherwise("CORE").alias("promo_code_zone"),
    )
    .orderBy(F.col("channel").asc_nulls_first(), "promo_code_zone")
)
