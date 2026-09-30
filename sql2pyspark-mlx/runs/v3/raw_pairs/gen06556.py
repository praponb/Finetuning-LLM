from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([4, 14, 17, 37]))
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Globex"])
    .agg(F.count("stock_qty"))
)
