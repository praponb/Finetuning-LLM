from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id")

deduped = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount"))).alias("success_total_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("amount_per_success"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("amount_per_bank_transfer"),
    )
)
