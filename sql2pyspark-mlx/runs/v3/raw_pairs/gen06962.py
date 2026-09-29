from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        "category",
        "product_name",
        F.when(F.col("stock_qty") >= 338, "High").when(F.col("stock_qty") >= 168, "Medium").otherwise("Low").alias("stock_qty_tier"),
    )
    .orderBy("product_name")
)
