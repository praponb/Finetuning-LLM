from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 3)
    .select("id", "unit_price")
)
