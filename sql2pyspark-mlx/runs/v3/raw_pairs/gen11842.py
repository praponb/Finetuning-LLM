from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 7)
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") > 800)
    .select("order_id", "total_unit_price")
    .orderBy("total_unit_price", "order_id")
)
