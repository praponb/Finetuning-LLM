from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-06-11 03:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.col("amount").cast("int").alias("amount_int"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "amount_int",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("amount_int") >= 72, "LARGE").otherwise("SMALL").alias("amount_int_tier"),
        "method",
        F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW").alias("session_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.round(F.sum("amount_int") / F.nullif(F.count(F.when(F.col("method") == "cash", 1)), F.lit(0)), 2).alias("amount_int_per_cash"),
    )
    .orderBy(F.col("amount_int_tier").asc_nulls_first())
)
