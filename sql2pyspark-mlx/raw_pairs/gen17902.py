from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "SUCCESS"]) & (~F.col("method").eqNullSafe(F.lit("promptpay"))))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.concat_ws("-", "method", "status").alias("method_status_key"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

cleaned_payments = (
    cleaned
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") >= F.lit("2023-05-20").cast("date")))
    .select("*", F.lower("method").alias("method_lower"))
)

with_windows = (
    cleaned_payments
    .filter(F.col("order_id").isin([13, 22, 27, 30]))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "method_filled",
        F.last("method", ignorenulls=True).over(w).alias("last_known_method"),
        F.first("method", ignorenulls=True).over(w_hist).alias("first_known_method"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "first_known_method",
        F.sum("is_new_session").over(w4).alias("session_id"),
    )
)

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "paid_at",
        "first_known_method",
        "session_id",
        F.when(F.col("method").rlike("^(cash|paypal)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("method_zone"),
    )
    .orderBy(F.col("session_id").desc())
)
