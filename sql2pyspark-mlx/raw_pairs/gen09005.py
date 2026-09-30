from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "cash"]) & F.col("method").isin(["promptpay", "bank_transfer", "cash"]))
    .groupBy("method")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy("method")
)
