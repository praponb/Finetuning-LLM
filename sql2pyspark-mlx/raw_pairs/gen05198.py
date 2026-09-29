from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Sports", "Grocery"])
    .agg(F.sum("stock_qty"))
)
