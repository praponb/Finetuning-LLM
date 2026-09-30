from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("unit_price") >= 120) & F.col("order_id").isin([4, 18, 34]) & (F.col("order_id") == 27))
)

order_id_summary = (
    base
    .groupBy("order_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    order_id_summary
    .select("order_id", "min_quantity")
    .orderBy("min_quantity", "order_id")
)
