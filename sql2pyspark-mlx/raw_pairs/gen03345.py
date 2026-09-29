from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([9, 29]))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
)
