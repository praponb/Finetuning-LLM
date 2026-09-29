from pyspark.sql import functions as F

max_stock_qty = df_products.filter(~F.col("brand").isin(["Globex", "Acme"])).agg(F.max("stock_qty")).first()[0]

result = (
    df_products
    .filter(F.col("stock_qty") >= max_stock_qty)
    .select("id", "stock_qty", "price")
    .orderBy("stock_qty")
)
