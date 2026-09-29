from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") >= 8)
    .select("supplier_id", "brand")
    .unionByName(
        df_products
        .filter(F.col("stock_qty") < 395)
        .select("supplier_id", "brand")
    )
    .distinct()
)
