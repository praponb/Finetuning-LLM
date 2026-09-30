from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Initech", "Stark", "Globex"])
    .agg(F.count("stock_qty"))
    .orderBy("category")
)
