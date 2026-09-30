from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("supplier_id").isNotNull())

agg = (
    recent_products
    .groupBy("supplier_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    agg
    .filter(F.col("cost_range") < 34)
    .orderBy(F.col("cost_range").desc(), "supplier_id")
    .limit(5)
)
