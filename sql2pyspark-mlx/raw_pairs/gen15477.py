from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1720, "high").when(F.col("amount") >= 330, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.countDistinct("amount").alias("unique_orders"))
    .filter(F.col("unique_orders") < 2)
)
