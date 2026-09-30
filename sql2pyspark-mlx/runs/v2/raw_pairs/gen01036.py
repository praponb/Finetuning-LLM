from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([19, 29, 31]))
    .select("id", "quantity")
    .orderBy(F.col("quantity").desc())
)
