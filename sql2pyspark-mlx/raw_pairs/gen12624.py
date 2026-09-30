from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")
w_recent = Window.partitionBy("order_id")
w3 = Window.partitionBy("carrier", "status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w_hist = Window.partitionBy(F.month("shipped_date"), "carrier").orderBy(F.col("cost").desc(), "id")
w6 = Window.partitionBy("promo_code", "order_id").orderBy(F.col("promo_id").desc_nulls_last(), "id", "promo_id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

enriched = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.sum("cost").over(w_recent).alias("order_id_sum_cost"),
        F.first("cost", ignorenulls=True).over(w3).alias("first_known_cost"),
        F.ntile(4).over(w4).alias("cost_bucket"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.status") == "COMPLETED")
    .select("src.*", F.col("p.status"), F.col("p.shipping_fee"), F.col("p.channel"))
)

base = (
    with_dims
    .filter((F.col("shipped_date") > F.lit("2023-09-08").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .withColumn("cost_rounded", F.round("cost", 0))
    .withColumn("next_cost_tier", F.when(F.col("next_cost") >= 99, "High").when(F.col("next_cost") >= 38, "Medium").otherwise("Low"))
)

filtered = (
    base
    .filter((F.col("shipped_date") >= F.lit("2023-08-22").cast("date")) & F.col("next_cost").between(36, 73))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

with_dims_shipments = (
    filtered
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.delivered_date",
        "e.cost",
        "e.next_cost",
        "e.order_id_sum_cost",
        "e.first_known_cost",
        "e.seq_no",
        "e.channel",
        "e.cost_filled",
        F.col("b.discount"),
    )
)

matched = (
    with_dims_shipments
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.shipped_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

ranked = (
    matched
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "next_cost",
        "order_id_sum_cost",
        "seq_no",
        "channel",
        "cost_filled",
        F.when(F.col("channel").rlike("^(store|web)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(mobile|partner)$"), "EXTENDED").otherwise("GROUP_B").alias("channel_zone"),
        F.col("sum_payment_amount"),
    )
    .orderBy(F.col("delivered_date").desc())
)
