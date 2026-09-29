from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

deduped = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy("order_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max(F.when(F.col("method") == "promptpay", F.col("amount"))).alias("promptpay_max_amount"),
    )
    .filter(F.col("promptpay_max_amount") < 1000000)
    .orderBy(F.col("promptpay_max_amount").desc_nulls_last())
)
