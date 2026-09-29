from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 30) | F.col("method").isin(["bank_transfer", "paypal", "card"]))
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
