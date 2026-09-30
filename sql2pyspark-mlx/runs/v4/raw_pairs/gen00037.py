from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.orderBy(F.col("amount").desc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_payments
    .select(
        "*",
        F.ntile(10).over(w).alias("amount_bucket"),
        F.min("amount").over(w2).alias("order_id_min_amount"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.percent_rank().over(w4).alias("amount_percent_rank"),
        F.avg("amount").over(w5).alias("amount_avg_6"),
    )
)

result = (
    with_windows
    .groupBy(
        "method",
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.when(F.col("amount") >= 1650, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.round(F.sum("amount_percent_rank") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("amount_percent_rank_per_paypal"),
    )
    .orderBy(F.col("method").desc_nulls_last())
)
