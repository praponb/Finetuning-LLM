from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.product_id").isin([5, 7]) | (F.col("ss.units") > 32))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
    )
    .orderBy("units", "id")
    .limit(5)
)
