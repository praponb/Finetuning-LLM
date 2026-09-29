from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.col("ss.revenue") <= 2400) | (F.col("p.category") == "Home"))
    .select(
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
    )
)
