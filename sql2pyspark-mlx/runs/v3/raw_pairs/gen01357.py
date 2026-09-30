from pyspark.sql import functions as F

src = (
    df_products
    .filter((F.col("price") < 790) & ((F.length("product_name") >= 5) | F.col("price").between(340, 1100)))
)

products_stats = (
    src
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("stock_qty")).otherwise(0)).alias("initech_stock_qty"),
    )
)

result = (
    products_stats
    .filter(F.col("initech_stock_qty") >= 65)
    .select("supplier_id", "initech_stock_qty")
)
