from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country", "store_id").orderBy("age", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("channel").isin(["store", "web", "partner"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        F.concat("status", F.lit("/"), "channel").alias("status_channel"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

enriched = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .select(
        "a.id",
        "a.customer_id",
        "a.store_id",
        "a.discount",
        "a.status_channel",
        F.col("c.country"),
        F.col("c.name"),
        F.col("c.age"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lag("age").over(w_part).alias("prev_age"),
        F.lag("discount").over(w_part).alias("prev_discount"),
        F.lead("discount").over(w_part).alias("next_discount"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity").between(4, 6)), "left_semi")
)

joined = (
    kept
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id").eqNullSafe(F.col("d.id")), "inner")
    .select("e.*", F.col("d.city"), F.col("d.opened_date"))
)

filtered = (
    joined
    .filter(F.col("city").isNotNull())
    .withColumn("next_discount_plus_prev_age", (F.col("next_discount") + F.col("prev_age")))
)

with_last = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.age",
        "s.prev_age",
        "s.prev_discount",
        "s.next_discount",
        "s.event_seq",
        "s.city",
        "s.opened_date",
        F.col("c.amount"),
    )
)

result = (
    with_last
    .select(
        "id",
        "name",
        "age",
        "prev_age",
        "next_discount",
        "city",
        "opened_date",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
)
