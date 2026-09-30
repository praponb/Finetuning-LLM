from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 990)
    .groupBy("supplier_id")
    .pivot("category", ["Fashion", "Electronics", "Home", "Grocery"])
    .agg(F.count("stock_qty"))
    .orderBy("supplier_id")
)
