from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(243, 267) & F.month("launch_date").isin([4, 5, 6]))
    .select("id", "category", "supplier_id", "price")
    .orderBy("price")
)
