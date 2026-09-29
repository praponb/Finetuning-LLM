from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy(F.year("paid_at"), "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("amount", "id")
w4 = Window.partitionBy("order_id")

enriched = (
    df_payments
    .filter(F.col("order_id").isin([10, 24]))
    .withColumn("amount_sum_7d", F.sum("amount").over(w))
    .withColumn("amount_count_30d", F.count("amount").over(w2))
    .withColumn("prev_amount", F.lag("amount", 1).over(w3))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w4)), 4))
)

result = (
    enriched
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount_sum_7d")).otherwise(0)).alias("paypal_amount_sum_7d"),
        F.count_if(F.col("status") == "SUCCESS").alias("success_events"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount_count_30d")).otherwise(0)).alias("refunded_amount_count_30d"),
        F.sum(F.when(F.col("method") == "card", F.col("prev_amount")).otherwise(0)).alias("card_prev_amount"),
    )
    .filter(F.col("success_events") > 1)
    .orderBy(F.col("order_id").desc_nulls_last())
)
