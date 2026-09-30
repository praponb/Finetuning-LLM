from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 3) & (F.col("order_id").isin([31, 34]) | F.col("product_id").isin([25, 34])))
    .select("quantity", "product_id", F.ceil("unit_price").alias("unit_price_whole"))
    .orderBy(F.col("quantity").desc(), F.col("product_id").desc())
)
