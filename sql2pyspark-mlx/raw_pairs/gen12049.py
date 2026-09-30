from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy(F.col("amount_whole").desc(), "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-13 19:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.floor("amount").alias("amount_whole"),
        F.when(F.col("amount") >= 1270, "High").when(F.col("amount") >= 210, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "left")
    .select("a.*", F.col("d.status"), F.col("d.amount").alias("order_amount"))
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w3))

with_dims = (
    sessionized
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.status").alias("order_status"), F.col("p.discount"))
)

result = (
    with_dims
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .withColumn("amount_whole_tier", F.when(F.col("amount_whole") >= 85, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["year_start", "amount_whole_tier"], ["year_start"]],
        "year_start",
        "amount_whole_tier",
    )
    .agg(
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.min("paid_at").alias("first_paid_at"),
        F.percentile_approx("session_id", 0.95).alias("session_id_p95"),
    )
    .filter(F.col("session_id_p95") > 0)
)
