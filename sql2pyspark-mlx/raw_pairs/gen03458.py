from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc_nulls_last(), "id")

ranked = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("status", F.date_trunc("WEEK", "paid_at").alias("week_start"))
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.min("paid_at").alias("first_seen"),
        F.avg(F.when(F.col("status") == "FAILED", F.col("amount"))).alias("failed_avg_amount"),
    )
)
