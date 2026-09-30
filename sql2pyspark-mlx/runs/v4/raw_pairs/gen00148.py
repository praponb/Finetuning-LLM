from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("shipping_fee").desc(), "id")
w_part = Window.partitionBy("channel").orderBy("amount", "id")
w_part_2 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w6 = Window.partitionBy("status").orderBy(F.col("discount_bucket").desc_nulls_last())
w7 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w8 = Window.partitionBy("status").orderBy("discount_bucket", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_orders
    .select(
        "*",
        F.row_number().over(w_grp).alias("event_seq"),
        F.row_number().over(w_part).alias("event_seq_2"),
        F.ntile(3).over(w_part_2).alias("discount_bucket"),
        F.corr("discount", "amount").over(w4).alias("corr_discount_amount"),
        F.row_number().over(w5).alias("event_seq_3"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.percent_rank().over(w6).alias("discount_bucket_percent_rank"),
        F.ntile(4).over(w7).alias("shipping_fee_bucket"),
        F.avg("amount").over(w8).alias("amount_avg_3"),
        F.sum("event_seq_2").over(w9).alias("running_event_seq_2"),
    )
)

with_window = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

summary = (
    with_window
    .groupBy(
        F.when(F.col("discount_pct") >= 0.3, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.max(F.when(F.col("status") == "SHIPPED", F.col("event_seq_2"))).alias("shipped_max_event_seq_2"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
    )
)

result = (
    summary
    .select(
        "discount_pct_tier",
        "activity_month",
        "shipped_rows",
        "distinct_promo_codes",
    )
)
