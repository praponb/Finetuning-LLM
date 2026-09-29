from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") > 210)
    .groupBy("category")
    .pivot("brand", ["Acme", "Umbrella", "Stark"])
    .agg(F.sum("stock_qty"))
    .orderBy("category")
)
