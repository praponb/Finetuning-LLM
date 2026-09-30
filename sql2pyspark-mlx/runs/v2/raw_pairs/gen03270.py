from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([18, 30]))
    .select("id", "unit_price", "order_id", "product_id")
)
