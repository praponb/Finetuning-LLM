from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.size("tags") > 2) & F.col("supplier_id").isNotNull())
    .groupBy("category")
    .agg(
        F.count("stock_qty").alias("stock_qty_count"),
        F.max("launch_date").alias("last_launch_date"),
    )
)
