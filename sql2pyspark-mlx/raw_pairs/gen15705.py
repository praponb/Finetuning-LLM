from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")

filtered = (
    df_orders
    .filter(F.col("order_date") > "2023-06-17")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    ranked
    .select("status")
    .exceptAll(
        df_orders
        .filter(F.col("discount") < 0.36)
        .select("status")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

attributed = (
    kept_rows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

kept = (
    attributed
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") >= 340), "left_semi")
)

joined = (
    kept
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.credit_limit") <= 13400)
    .select("s.*", F.col("d.age"))
)

result = (
    joined
    .select(
        "channel",
        "discount",
        "shipping_fee",
        F.col("discount_pct").alias("DISCOUNT_PCT"),
    )
    .unpivot(
        ids=["channel"],
        values=["discount", "shipping_fee", "DISCOUNT_PCT"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
