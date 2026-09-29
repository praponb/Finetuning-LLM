from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("is_success", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("method").orderBy("running_sum_amount", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy(F.date_trunc("MONTH", "paid_at"), "method").orderBy("running_max_amount", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("running_min_amount", "id")
w6 = Window.partitionBy("method").orderBy(F.col("next_running_count_amount").asc_nulls_last(), "id")
w_running = Window.partitionBy("order_id").orderBy("running_min_amount_avg_2", "id")
w_ordered = Window.partitionBy("order_id", "method").orderBy("running_max_amount_count_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("running_sum_amount_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-06-22 09:00:00"))) & F.col("order_id").isNull())
    .select(
        "*",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("order_id").isin([1, 8, 35]))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.min("amount").over(w).alias("running_min_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "order_id",
        "method",
        "running_max_amount",
        F.avg("running_sum_amount").over(w_recent).alias("running_sum_amount_avg_6"),
        F.avg("running_min_amount").over(w_hist).alias("running_min_amount_avg_2"),
        F.count("running_max_amount").over(w4).alias("running_max_amount_count_90d"),
        F.lead("running_count_amount", 2).over(w5).alias("next_running_count_amount"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.lead("next_running_count_amount", 2, 0).over(w_running).alias("next_next_running_count_amount"),
        F.first("next_running_count_amount", ignorenulls=True).over(w_ordered).alias("first_known_next_running_count_amount"),
        F.sum("running_min_amount_avg_2").over(w9).alias("cumulative_running_min_amount_avg_2"),
    )
)

result = (
    enriched
    .groupBy(
        "method",
        F.when(F.col("running_min_amount_avg_2") >= 1690, "HIGH").otherwise("LOW").alias("running_min_amount_avg_2_tier"),
    )
    .agg(
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sort_array(F.collect_set("method")).alias("method_list_2"),
        F.min("next_next_running_count_amount").alias("min_next_next_running_count_amount"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_rows"),
    )
    .orderBy(F.col("running_min_amount_avg_2_tier").desc_nulls_first())
)
