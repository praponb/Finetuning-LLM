from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("revenue") < 1700)
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("ps.max_units"),
        F.col("ps.last_sale_date"),
    )
)
