from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy("created_at", "id", "item_no")
w2 = Window.partitionBy("promo").orderBy("created_at", "id", "item_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("item.product_id"),
        F.col("item.promo"),
    )
)

deduped = (
    exploded
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_prev = deduped.withColumn("prev_created_at", F.lag("created_at").over(w))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_created_at").isNull() | (F.col("created_at") > F.col("prev_created_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_carts = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

filtered = (
    sessions_carts
    .filter(F.col("is_new_session").between(0, 1))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "item_no",
        "product_id",
        "promo",
        "prev_created_at",
        "is_new_session",
        "session_id",
        F.dayofmonth("created_at").alias("created_at_day"),
        F.floor("is_new_session").alias("is_new_session_whole"),
        F.coalesce("promo", F.lit("N/A")).alias("resolved_promo"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.coupon_codes",
        "e.item_no",
        "e.is_new_session",
        "e.session_id",
        "e.created_at_day",
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
)

base = (
    enriched
    .filter(F.col("is_new_session") > 0)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "item_no",
        "is_new_session",
        "session_id",
        "created_at_day",
        "credit_limit",
        "country",
        F.col("created_at_day").cast("string").alias("created_at_day_str"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_filled"),
    )
)

result = (
    base
    .groupBy("customer_id")
    .agg(
        F.sum("item_no").alias("total_item_no"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
    .filter(F.col("avg_credit_limit") > 0)
    .orderBy(F.col("total_item_no").desc_nulls_last())
)
