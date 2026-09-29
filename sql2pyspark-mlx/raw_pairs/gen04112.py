from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("method").isin(["bank_transfer", "paypal"]), "priority").otherwise("other").alias("method_group"),
        F.when(F.col("amount") >= 1450, "high").when(F.col("amount") >= 700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("unique_methods") >= 2)
)
