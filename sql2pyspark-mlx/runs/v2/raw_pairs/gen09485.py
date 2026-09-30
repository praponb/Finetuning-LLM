from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "quantity", "order_id")
    .orderBy(F.col("quantity").desc())
)
