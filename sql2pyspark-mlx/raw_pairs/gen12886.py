from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isin([5, 17, 36]) & (F.col("product_id") == 13) & F.col("product_id").isNotNull())
)

summary = (
    base
    .groupBy("order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    summary
    .filter(F.col("quantity_range") > 43)
    .select("order_id", "quantity_range")
    .orderBy(F.col("quantity_range").desc(), "order_id")
    .limit(1)
)
