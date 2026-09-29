from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 7)
    .orderBy(F.col("unit_price").desc(), "id")
    .limit(10)
)
