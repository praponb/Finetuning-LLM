from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.revenue") < 3200) & F.col("ss.revenue").between(1400, 3500))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
    )
)
