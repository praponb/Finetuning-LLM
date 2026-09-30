from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([18, 21, 24, 39]) & (F.col("unit_price") >= 310) & (F.col("unit_price") > 360))
    .select("id", "order_id", "unit_price")
    .orderBy(F.col("id").desc())
)
