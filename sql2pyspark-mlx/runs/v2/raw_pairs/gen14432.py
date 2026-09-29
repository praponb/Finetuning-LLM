from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("stock_qty") > 242) & (F.col("supplier_id").isNull() | (F.size("tags") > 1)))
)

summary = (
    base
    .groupBy("category")
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = summary.select("category", "last_launch_date")
