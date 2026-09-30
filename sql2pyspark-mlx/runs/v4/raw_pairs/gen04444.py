from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_hist = Window.orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("method").orderBy("amount", "id")
w5 = Window.partitionBy("order_id", "method").orderBy(F.col("amount").asc_nulls_last())
w6 = Window.partitionBy("method", "status").orderBy("paid_at_day", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("amount_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("amount_rank").asc_nulls_last())
w_running = Window.orderBy(F.col("last_known_amount").desc(), "id")
w10 = Window.partitionBy("method", "status").orderBy(F.col("paid_at").asc_nulls_first(), "id")

cleaned = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-09-01")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.last("amount", ignorenulls=True).over(w_hist).alias("last_known_amount"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.rank().over(w5).alias("amount_rank"),
    )
)

metrics = (
    scored
    .withColumn("amount_rank_avg_5", F.avg("amount_rank").over(w6))
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w7))
    .withColumn("amount_rank_percent_rank", F.percent_rank().over(w8))
    .withColumn("last_known_amount_bucket", F.ntile(3).over(w_running))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w10))
    .filter(F.col("row_num") == 1)
)

base = (
    latest
    .filter(F.col("method").isNotNull() & F.col("amount_rank_avg_5").between(5, 7))
    .withColumn("method_filled", F.coalesce("method", F.lit("Unknown")))
    .withColumn("method_filled_2", F.coalesce("method", F.lit("UNKNOWN")))
)

result = (
    base
    .groupBy(
        F.date_trunc("quarter", "paid_at").alias("quarter_start"),
        F.date_trunc("week", "paid_at").alias("week_start"),
        F.when(F.col("amount_rank_avg_5") >= 7, "LARGE").otherwise("SMALL").alias("amount_rank_avg_5_tier"),
    )
    .agg(
        F.sum("row_seq").alias("total_row_seq"),
        F.sum(F.when(F.col("method") == "cash", F.col("row_seq")).otherwise(0)).alias("cash_row_seq"),
    )
    .filter(F.col("cash_row_seq") > 0)
    .orderBy(F.col("amount_rank_avg_5_tier").asc_nulls_first())
)
