from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "order_id")
w2 = Window.partitionBy(F.to_date("paid_at")).orderBy(F.col("amount_scaled").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("WEEK", "paid_at"), "order_id")
w5 = Window.partitionBy("status", "order_id").orderBy(F.col("status_count_amount_scaled").desc())
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

filtered = (
    df_payments
    .filter(F.col("method").eqNullSafe(F.lit("paypal")))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.lower("method").alias("method_lower"),
        F.round((F.col("amount") * 1.1), 2).alias("amount_scaled"),
    )
)

matched_keys = (
    filtered
    .select("id", "status")
    .subtract(
        df_payments
        .filter(F.col("amount") <= 1560)
        .select("id", "status")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.to_date("paid_at") >= "2025-01-01")
    .select(
        "*",
        F.count("amount_scaled").over(w_ordered).alias("status_count_amount_scaled"),
        F.cume_dist().over(w2).alias("amount_scaled_cume_dist"),
        F.avg("amount_scaled").over(w3).alias("amount_scaled_avg_7d"),
    )
)

scored_payments = (
    scored
    .withColumn("paid_at_period_max_amount_scaled", F.max("amount_scaled").over(w4))
    .withColumn("status_count_amount_scaled_cume_dist", F.cume_dist().over(w5))
)

enriched = (
    scored_payments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select(
        "a.order_id",
        "a.paid_at",
        "a.method_lower",
        "a.amount_scaled_avg_7d",
        F.col("b.amount"),
        F.col("b.order_date"),
    )
)

report = (
    enriched
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "method_lower",
        "amount_scaled_avg_7d",
        "amount",
        "order_date",
        F.col("avg_order_item_unit_price"),
    )
)

result = (
    report
    .select("amount_scaled_avg_7d", "amount", "order_date", "avg_order_item_unit_price")
)
