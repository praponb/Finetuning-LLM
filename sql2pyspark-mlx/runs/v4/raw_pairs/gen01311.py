from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_part = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.substring("method", 1, 3).alias("method_prefix"))
)

kept_keys = (
    filtered
    .select("order_id", "method")
    .subtract(
        df_payments
        .filter(F.col("method") == "promptpay")
        .select("order_id", "method")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("amount").over(w_part).alias("amount_sum_90d"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.discount"))
)

summary = (
    with_dims
    .groupBy(F.date_trunc("month", "paid_at").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount_sum_90d")).otherwise(0)).alias("failed_amount_sum_90d"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount_share")).otherwise(0)).alias("refunded_amount_share"),
    )
)

result = (
    summary
    .select("activity_month", "method_list", "row_count", "refunded_amount_share")
)
