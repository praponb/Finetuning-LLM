from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("order_id", "order_status").orderBy("running_count_discount", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("order_status", "order_id").orderBy("discount_count_90d", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("order_status", "order_id").orderBy(F.col("last7_count_discount_count_90d").asc_nulls_first(), "id")

valid = (
    df_payments
    .filter(F.col("amount").between(1420, 1440) & F.col("method").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["FAILED", "SUCCESS"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.order_date"))
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.store_id").isin([3, 19, 35]))
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.paid_at",
        "a.status",
        "a.method_filled",
        "a.order_date",
        F.col("b.status").alias("order_status"),
    )
)

enriched_payments = (
    joined
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.paid_at",
        "src.method_filled",
        "src.order_date",
        "src.order_status",
        F.col("d.discount"),
        F.col("d.order_date").alias("order_order_date"),
    )
)

scored = (
    enriched_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "order_status",
        F.count("discount").over(w).alias("running_count_discount"),
        F.sum("discount").over(w).alias("running_sum_discount"),
        F.max("discount").over(w).alias("running_max_discount"),
        F.count("discount").over(w2).alias("discount_count_90d"),
    )
)

scored_payments = (
    scored
    .select(
        "*",
        F.count("discount_count_90d").over(w3).alias("last7_count_discount_count_90d"),
        F.avg("discount_count_90d").over(w3).alias("last7_avg_discount_count_90d"),
        F.max("discount_count_90d").over(w3).alias("last7_max_discount_count_90d"),
        F.avg("discount_count_90d").over(w4).alias("discount_count_90d_avg_6_prior"),
    )
)

latest = (
    scored_payments
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "order_status",
        F.col("running_max_discount").alias("RUNNING_MAX_DISCOUNT_VAL"),
        "last7_avg_discount_count_90d",
        "running_sum_discount",
    )
    .unpivot(
        ids=["id", "order_status"],
        values=["RUNNING_MAX_DISCOUNT_VAL", "last7_avg_discount_count_90d", "running_sum_discount"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
