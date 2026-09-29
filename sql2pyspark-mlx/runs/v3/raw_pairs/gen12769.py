from pyspark.sql import functions as F

max_stock_qty = df_products.filter(F.col("supplier_id").isin([19, 24, 25, 28])).agg(F.max("stock_qty")).first()[0]

result = (
    df_products
    .filter(F.col("stock_qty") >= max_stock_qty)
    .select("id", "stock_qty", "product_name", "cost")
    .orderBy(F.col("cost").desc(), F.col("id").desc())
)
