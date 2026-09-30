from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Acme", "Umbrella", "Globex"]))
    .groupBy("supplier_id", "brand", "category")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
    .orderBy(F.col("avg_stock_qty").desc(), "supplier_id", "brand", "category")
    .limit(1)
)
