from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") >= 770) & F.col("supplier_id").isin([27, 28]))
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("stock_qty_count").desc())
)
