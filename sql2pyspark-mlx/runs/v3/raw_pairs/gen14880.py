from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") == 241)
    .groupBy("category")
    .pivot("brand", ["Acme", "Globex"])
    .agg(F.count("cost"))
)
