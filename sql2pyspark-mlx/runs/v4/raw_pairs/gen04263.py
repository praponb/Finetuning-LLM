from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 23)
    .select("order_id", "avg_quantity")
    .orderBy(F.col("order_id").desc())
    .limit(20)
)
