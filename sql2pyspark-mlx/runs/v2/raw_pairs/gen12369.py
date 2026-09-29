from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty").between(281, 313) & F.col("p.launch_date").between("2023-04-01", "2023-07-14"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("p.price"),
    )
)
