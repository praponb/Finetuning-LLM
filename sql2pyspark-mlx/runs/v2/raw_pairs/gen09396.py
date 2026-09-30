from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 24, 28, 38]) & (F.col("quantity") != 5) & F.col("unit_price").between(300, 340))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.15), 2))
    .withColumn("unit_price_rounded", F.round("unit_price", 1))
    .orderBy(F.col("order_id").desc())
)
