from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("method") == "card") & (F.col("amount") < 830)) | F.col("order_id").isin([20, 27, 37, 40]))
    .groupBy("status")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_methods"),
    )
)
