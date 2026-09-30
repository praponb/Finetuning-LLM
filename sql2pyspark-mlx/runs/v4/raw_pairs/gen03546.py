from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter((F.col("ss.sale_date") >= "2023-03-01") & ~F.col("p.category").isin(["Books", "Electronics"]))
    .select(
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
    )
)
