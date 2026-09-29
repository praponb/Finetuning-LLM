from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull() & (F.size("tags") >= 1))
    .groupBy("category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
    .orderBy("min_launch_date", "category")
    .limit(5)
)
