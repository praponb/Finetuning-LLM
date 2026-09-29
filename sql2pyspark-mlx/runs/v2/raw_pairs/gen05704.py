from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([15, 16, 19, 29]) & (F.col("quantity") >= 7) & F.col("product_id").isin([15, 22, 29, 31]))
    .select("id", "quantity", "unit_price", "order_id")
    .orderBy(F.col("order_id").desc(), "id")
    .limit(1)
)
