from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 14) & (F.to_date("paid_at") < "2023-06-12") & (F.col("method") != "promptpay"))
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
    .orderBy("success_count")
)
