from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.max("launch_date").alias("last_launch_date"),
    )
    .filter(F.col("stock_qty_range") > 62)
    .select("supplier_id", "stock_qty_range", "last_launch_date")
    .orderBy("last_launch_date")
)
