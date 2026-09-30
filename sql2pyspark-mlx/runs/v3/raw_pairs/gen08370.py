from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("product_id").isin([23, 37, 40]) & F.col("product_id").isNull()))
    .select("id", "order_id", "product_id")
)
