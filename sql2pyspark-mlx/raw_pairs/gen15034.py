from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("years_since_paid_at").desc_nulls_first(), "id")
w_grp = Window.partitionBy(F.to_date("paid_at"), "method").orderBy("latest_rank", "id")
w_grp_2 = Window.orderBy("years_since_paid_at", "id")
w4 = Window.partitionBy("method").orderBy("years_since_paid_at")
w5 = Window.partitionBy("order_id").orderBy("next_amount")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        (F.datediff(F.current_date(), "paid_at") / 365.25).alias("years_since_paid_at"),
        F.trim("status").alias("clean_status"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid_payments = (
    ranked
    .filter(F.col("order_id").isNotNull() & (F.col("amount") >= 840))
    .withColumn("years_since_paid_at_tier", F.when(F.col("years_since_paid_at") >= 72, "High").when(F.col("years_since_paid_at") >= 23, "Medium").otherwise("Low"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("UNKNOWN")))
)

enriched = (
    valid_payments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.status"))
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("years_since_paid_at", 1).over(w_grp).alias("prev_years_since_paid_at"),
        F.lead("amount", 1).over(w_grp_2).alias("next_amount"),
        F.percent_rank().over(w4).alias("years_since_paid_at_percent_rank"),
    )
)

ranked_payments = (
    metrics
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked_payments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "years_since_paid_at",
        "clean_status",
        "latest_rank",
        "years_since_paid_at_tier",
        "resolved_method",
        F.when(F.col("method").rlike("^(card|paypal)$"), "OTHER_ZONE").when(F.col("method").rlike("^(bank_transfer|cash)$"), "CORE").otherwise("PRIORITY").alias("method_zone"),
        F.col("min_order_item_unit_price"),
    )
)
