from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Globex"])
    .agg(F.avg("stock_qty"))
)
