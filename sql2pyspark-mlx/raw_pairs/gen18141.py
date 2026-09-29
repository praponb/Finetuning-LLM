from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.to_date(F.lit("2023-08-09")))
    .select(
        "*",
        F.when(F.col("status").isin(["REFUNDED", "FAILED"]), "Group A").otherwise("Other").alias("status_group"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "method",
        "paid_at",
        "method_filled",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.when(F.col("session_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.sum(F.when(F.col("method") == "card", F.col("session_no")).otherwise(0)).alias("card_session_no"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("session_no_per_bank_transfer"),
    )
    .filter(F.col("card_session_no") < 1000000)
)
