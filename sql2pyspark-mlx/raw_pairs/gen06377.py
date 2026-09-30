from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").desc_nulls_last(), "id")

deduped = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("amount") >= 1250, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .orderBy(F.col("failed_amount").desc_nulls_last())
)
