from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 270) & (F.col("order_id").isin([21, 24, 30, 40]) | F.col("quantity").isNotNull()))
    .select("id", "order_id", "quantity")
    .orderBy(F.col("order_id").desc(), F.col("quantity").desc(), "id")
    .limit(5)
)
