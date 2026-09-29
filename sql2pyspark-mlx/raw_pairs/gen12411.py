from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost").between(380, 640) | F.col("p.cost").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.date_format(F.col("p.launch_date"), "yyyy-MM-dd").alias("launch_date_label"),
    )
)
