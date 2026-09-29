from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 370) & (F.col("quantity") != 4))
    .select("id", "quantity", "unit_price")
    .orderBy("unit_price", "id")
    .limit(1)
)
