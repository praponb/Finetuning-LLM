from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 3)
    .groupBy(
        F.when(F.col("method").isin(["promptpay", "cash"]), "tier_1").otherwise("other").alias("method_group"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("unique_methods") >= 5)
    .select("method_group", "unique_methods", "avg_amount")
)
