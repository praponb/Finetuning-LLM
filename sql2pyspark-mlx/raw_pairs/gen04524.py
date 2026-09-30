from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-06-19").cast("date"))
    .select(
        "*",
        F.when(F.col("discount") >= 0.44, "High").when(F.col("discount") >= 0.19, "Medium").otherwise("Low").alias("discount_band"),
        F.trim("status").alias("clean_status"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("payment_status"),
        F.col("c.method").alias("payment_method"),
    )
)

enriched = (
    latest_match
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.opened_date"), F.col("b.city"), F.col("b.store_name"))
)

with_last = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier"))
)

matched = (
    with_last
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("s.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

kept_keys = (
    matched
    .select("store_id")
    .subtract(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("store_id")
    )
)

kept_rows = (
    matched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 37, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .groupingSets(
        [["shipping_fee_tier", "year_start"], ["shipping_fee_tier"], []],
        "shipping_fee_tier",
        "year_start",
    )
    .agg(
        F.sort_array(F.collect_set("payment_status")).alias("payment_status_list"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select("year_start", "payment_status_list", "num_cities", "shipping_fee_p90")
    .orderBy(F.col("year_start").desc())
)
