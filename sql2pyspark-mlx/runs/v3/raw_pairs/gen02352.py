from pyspark.sql import functions as F

src = df_products.filter(F.col("cost").between(380, 390))

supplier_id_summary = (
    src
    .groupBy("supplier_id")
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)

result = (
    supplier_id_summary
    .filter(F.col("avg_stock_qty") > 170)
    .select("supplier_id", "avg_stock_qty", "min_stock_qty")
)
