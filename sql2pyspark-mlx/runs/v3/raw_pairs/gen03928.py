from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_stores"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
)
