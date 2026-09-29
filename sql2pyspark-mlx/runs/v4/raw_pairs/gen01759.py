from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([8, 18, 20]) & F.col("brand").isin(["Initech", "Globex", "Stark"]))
    .select("brand", "tags", F.ceil("stock_qty").alias("stock_qty_whole"))
    .orderBy("brand")
)
