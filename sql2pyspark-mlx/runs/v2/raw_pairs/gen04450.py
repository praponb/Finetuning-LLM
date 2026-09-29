from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 17) & (F.col("unit_price") >= 140))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low"))
)
