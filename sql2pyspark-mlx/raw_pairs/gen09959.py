from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-27 20:00:00").cast("timestamp")) & F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.concat_ws("-", "method", "status").alias("method_status_key"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1210, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "amount_tier"], ["year_start"], []],
        "year_start",
        "amount_tier",
    )
    .agg(
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("burst_id_per_bank_transfer"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.min("paid_at").alias("first_paid_at"),
        F.max("paid_at").alias("last_paid_at"),
    )
)
