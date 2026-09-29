from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Home", "Books", "Fashion", "Grocery"])
    .agg(F.max("stock_qty"))
)
