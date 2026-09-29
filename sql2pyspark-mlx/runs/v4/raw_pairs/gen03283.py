from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-03-19").cast("date"))
    .select("*", F.greatest("cost", "stock_qty").alias("greatest_cost_stock_qty"))
)

result = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") > "2024-02-01"), "left_anti")
)
