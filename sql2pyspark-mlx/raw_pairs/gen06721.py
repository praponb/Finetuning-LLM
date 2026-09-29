from pyspark.sql import functions as F

avg_price = df_products.filter(F.array_contains("tags", "clearance")).agg(F.avg("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") > avg_price)
    .select("id", "price", "supplier_id", "stock_qty")
    .orderBy("stock_qty")
)
