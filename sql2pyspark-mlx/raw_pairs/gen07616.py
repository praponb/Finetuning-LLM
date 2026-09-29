from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("product_name").like("Wa%"))
    .select(
        "id",
        "brand",
        "price",
        "supplier_id",
        (F.col("stock_qty") + F.col("price")).alias("stock_qty_plus_price"),
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)
