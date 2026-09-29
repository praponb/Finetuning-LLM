from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy(F.month("paid_at"))
w3 = Window.partitionBy(F.year("paid_at")).orderBy(F.col("amount_rounded").desc())
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-07-17 04:00:00"))) & F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round("amount", 1).alias("amount_rounded"),
        F.trim("method").alias("clean_method"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

filtered_payments = (
    filtered
    .filter(F.col("paid_at") > F.lit("2023-09-19").cast("date"))
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

remaining_keys = (
    filtered_payments
    .select("status")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isin([12, 22, 32, 39]))
        .select("status")
    )
)

reconciled = (
    filtered_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.avg("amount_int").over(w).alias("amount_int_avg_90d"),
        F.min("amount").over(w2).alias("paid_at_month_min_amount"),
        F.dense_rank().over(w3).alias("amount_rounded_dense_rank"),
        F.max("amount_rounded").over(w4).alias("amount_rounded_max_7d"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.amount_rounded",
        "a.amount_int",
        "a.resolved_method",
        "a.amount_int_avg_90d",
        "a.paid_at_month_min_amount",
        "a.amount_rounded_max_7d",
        F.col("p.status").alias("order_status"),
    )
)

result = (
    with_dims
    .cube(
        F.when(F.col("amount_rounded") >= 76, "PREMIUM").otherwise("STANDARD").alias("amount_rounded_tier"),
        "order_id",
        F.when(F.col("amount_int_avg_90d") >= 68, "LARGE").otherwise("SMALL").alias("amount_int_avg_90d_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.sum(F.when(F.col("order_status") == "PENDING", F.col("amount_int")).otherwise(0)).alias("pending_amount_int"),
        F.sum(F.when(F.col("method") == "cash", F.col("amount_rounded_max_7d")).otherwise(0)).alias("cash_amount_rounded_max_7d"),
    )
)
