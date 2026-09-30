from pyspark.sql import functions as F

avg_stock_qty = df_products.filter(F.col("cost").between(390, 520)).agg(F.avg("stock_qty")).first()[0]

result = (
    df_products
    .filter(F.col("stock_qty") >= avg_stock_qty)
    .select("id", "stock_qty")
    .orderBy(F.col("id").desc())
)
