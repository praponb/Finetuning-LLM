from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("stock_qty") > 379).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
    )
    .orderBy(F.col("revenue").desc(), F.col("id").desc())
    .limit(50)
)
