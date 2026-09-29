from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 341)
    .groupBy(
        "category",
        F.when(F.col("stock_qty") >= 420, "high").when(F.col("stock_qty") >= 158, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("launch_date").alias("last_launch_date"),
    )
    .filter(F.col("cnt") > 8)
    .select("category", "stock_qty_bucket", "cnt", "last_launch_date")
)
