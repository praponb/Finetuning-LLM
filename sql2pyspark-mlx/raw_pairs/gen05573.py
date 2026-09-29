from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Acme"]))
    .groupBy("category")
    .pivot("brand", ["Globex", "Umbrella", "Acme", "Initech"])
    .agg(F.count("stock_qty"))
)
