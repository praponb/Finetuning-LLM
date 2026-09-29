from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("category", ["Sports", "Books", "Home"])
    .agg(F.avg("stock_qty"))
)
