from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("units").between(14, 19))

product_id_summary = (
    base
    .groupBy("product_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
)

result = (
    product_id_summary
    .select("product_id", "units_range")
    .orderBy("product_id")
)
