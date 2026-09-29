from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 365, "high").when(F.col("p.stock_qty") >= 195, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
    .orderBy("stock_qty_bucket")
)
