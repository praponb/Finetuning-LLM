from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Fashion"]))
    .groupBy("category")
    .pivot("brand", ["Stark", "Globex", "Umbrella"])
    .agg(F.sum("stock_qty"))
    .orderBy("category")
)
