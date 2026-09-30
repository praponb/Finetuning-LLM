from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("paid_at") < "2024-01-03") & F.col("amount").between(1230, 1430)) | F.col("order_id").isNotNull())
    .groupBy(
        F.when(F.col("method").isin(["paypal", "cash"]), "tier_1").otherwise("other").alias("method_group"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 62)
    .orderBy("method_group")
)
