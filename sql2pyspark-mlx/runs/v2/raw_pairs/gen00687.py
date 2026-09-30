from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "price",
        "cost",
        F.add_months("launch_date", 6).alias("launch_date_plus_months"),
        (F.col("cost") - F.col("stock_qty")).alias("cost_minus_stock_qty"),
    )
)
