from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "order_id",
        F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_tier"),
    )
    .orderBy("order_id")
)
