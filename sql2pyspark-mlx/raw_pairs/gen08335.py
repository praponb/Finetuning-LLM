from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "status").orderBy("session_no")
w4 = Window.partitionBy("method").orderBy(F.col("session_no").desc())
w5 = Window.orderBy("session_no")

valid = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("paypal"))) & F.hour("paid_at").between(10, 18))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.upper("method").alias("method_upper"),
        F.when(F.col("amount") >= 1560, "High").when(F.col("amount") >= 450, "Medium").otherwise("Low").alias("amount_tier"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

valid_payments = (
    valid
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-05-23"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "method_upper",
        "amount_tier",
        "method_filled",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled_2"),
    )
)

flagged = (
    valid_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "method_upper",
        "method_filled",
        "paid_at_day",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

matched_keys = (
    sessionized
    .select("method", "id")
    .exceptAll(
        df_payments
        .filter(F.col("order_id").isin([20, 36]))
        .select("method", "id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("method").isNull())
    .select(
        "*",
        F.rank().over(w3).alias("session_no_rank"),
        F.rank().over(w4).alias("session_no_rank_2"),
        F.rank().over(w5).alias("session_no_rank_3"),
    )
)

stats = (
    enriched
    .withColumn("session_no_rank_3_tier", F.when(F.col("session_no_rank_3") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["session_no", "session_no_rank_3_tier"], ["session_no"], []],
        "session_no",
        "session_no_rank_3_tier",
    )
    .agg(
        F.round(F.sum("session_no_rank_3") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("session_no_rank_3_per_success"),
        F.sum(F.when(F.col("method") == "paypal", F.col("session_no_rank_3")).otherwise(0)).alias("paypal_session_no_rank_3"),
        F.max("paid_at").alias("last_paid_at"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "session_no",
        "session_no_rank_3_tier",
        "session_no_rank_3_per_success",
        "last_paid_at",
        "grouping_id",
    )
    .orderBy(F.col("session_no_rank_3_per_success").desc())
)
