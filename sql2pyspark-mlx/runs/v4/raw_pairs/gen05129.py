from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("order_id", "method").orderBy(F.col("next_amount").desc_nulls_last(), "id")
w6 = Window.partitionBy("status").orderBy(F.col("latest_rank_per_cumulative_amount").asc_nulls_last(), "id")
w7 = Window.partitionBy("method").orderBy("paid_at_period", "id")
w8 = Window.partitionBy("method").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status").orderBy("paid_at_period", "id")
w10 = Window.partitionBy("status").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_payments
    .select(
        "*",
        F.count("amount").over(w).alias("order_id_count_amount"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.lead("amount", 1).over(w3).alias("next_amount"),
        F.count("amount").over(w4).alias("amount_count_7d"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("paid_at") > F.lit("2024-03-27 15:00:00").cast("timestamp"))
    .select(
        "id",
        "method",
        "amount",
        "status",
        "amount_count_7d",
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
        F.round((F.col("latest_rank") / F.col("cumulative_amount")), 2).alias("latest_rank_per_cumulative_amount"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

flagged = (
    deduped
    .withColumn("new_group_flag", F.when(F.lag("paid_at_period").over(w7).isNull() | (F.col("paid_at_period") > F.lag("paid_at_period").over(w7) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w8))

flagged_2 = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("paid_at_period").over(w9).isNull() | (F.col("paid_at_period") > F.lag("paid_at_period").over(w9) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized_payments = flagged_2.withColumn("session_no", F.sum("is_new_burst").over(w10))

stats = (
    sessionized_payments
    .withColumn("year_start", F.date_trunc("year", "paid_at_period"))
    .rollup("status", "year_start")
    .agg(
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("new_group_flag"))).alias("bank_transfer_total_new_group_flag"),
        F.grouping("status").alias("gid"),
    )
)

result = (
    stats
    .select("row_count", "bank_transfer_total_new_group_flag")
    .orderBy(
        F.col("bank_transfer_total_new_group_flag").desc_nulls_last(),
        F.col("row_count").desc(),
    )
)
