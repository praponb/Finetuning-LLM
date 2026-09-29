from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("stock_qty")
    .unionByName(
        df_products
        .filter(F.col("stock_qty").between(115, 255))
        .select("stock_qty")
    )
    .distinct()
    .orderBy("stock_qty")
)
