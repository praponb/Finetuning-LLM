from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 130) & (F.col("unit_price") < 400))
    .select("id", "unit_price")
)
