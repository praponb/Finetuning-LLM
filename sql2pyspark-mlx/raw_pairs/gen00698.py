from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Sports")
    .select(
        "id",
        "supplier_id",
        "brand",
        "cost",
        F.length("product_name").alias("product_name_length"),
        F.round("stock_qty", 2).alias("stock_qty_rounded"),
    )
    .orderBy("brand", F.col("supplier_id").desc())
)
