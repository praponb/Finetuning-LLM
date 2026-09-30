from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("quantity") == 6)
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 1.07), 2))
)

agg = (
    base
    .groupBy("product_id")
    .agg(
        (F.max("quantity_scaled") - F.min("quantity_scaled")).alias("quantity_scaled_range"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    agg
    .filter(F.col("total_unit_price") >= 1000)
    .orderBy(F.col("product_id").desc())
)
