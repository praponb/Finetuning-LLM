from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 21]) | F.col("quantity").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.when(F.col("unit_price") >= 330, "High").when(F.col("unit_price") >= 70, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
    .orderBy("quantity", "id")
    .limit(10)
)
