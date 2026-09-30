from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "FAILED") & F.col("order_id").isin([7, 13, 23]) & F.hour("paid_at").between(9, 15))
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("amount").alias("avg_amount"),
    )
)
