from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(11, 17) | F.col("store_id").isin([29, 32]))
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)
