from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(F.col("suppliers.country"))
    .agg(
        F.min(F.col("products.launch_date")).alias("first_launch_date"),
        (F.max(F.col("products.stock_qty")) - F.min(F.col("products.stock_qty"))).alias("stock_qty_range"),
    )
    .orderBy(F.col("country").desc())
)
