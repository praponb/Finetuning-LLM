from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(104, 178))
    .select("launch_date")
    .union(df_shipments.filter(F.col("cost") > 58).select("shipped_date"))
)
