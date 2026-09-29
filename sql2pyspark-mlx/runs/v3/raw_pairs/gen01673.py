from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]) | (F.col("paid_at") >= "2024-12-01"))
    .groupBy(
        F.when(F.col("method").isin(["paypal", "cash"]), "core").otherwise("other").alias("method_group"),
        "method",
    )
    .agg(F.countDistinct("amount").alias("unique_methods"))
    .filter(F.col("unique_methods") < 5)
)
