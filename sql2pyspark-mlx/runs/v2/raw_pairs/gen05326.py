from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([18, 20, 37]) & F.col("unit_price").between(230, 400))
    .select(
        "id",
        "product_id",
        "order_id",
        "unit_price",
        F.when(F.col("quantity") >= 6, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_band"),
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)
