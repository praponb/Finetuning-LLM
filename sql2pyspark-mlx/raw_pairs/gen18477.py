from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

deduped = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "method", "paid_at", "status")
)

result = (
    deduped
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.count_if(F.col("status") == "SUCCESS").alias("success_events"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") > 1)
)
