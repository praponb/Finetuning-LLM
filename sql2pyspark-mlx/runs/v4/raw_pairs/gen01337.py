from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Grocery") & (F.col("cost") <= 410))
    .select(
        "cost",
        "supplier_id",
        "launch_date",
        F.least("price", "stock_qty").alias("least_price_stock_qty"),
    )
)
