from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
