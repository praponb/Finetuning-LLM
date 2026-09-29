from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.sale_date") < "2023-05-04") | (F.col("ss.store_id") == 3))
    .select(
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.store_id"),
    )
)
