from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(150, 160))
    .select("id", "quantity")
)
