from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("p.cost"),
        F.round((F.col("ss.revenue") * 1.1), 2).alias("adjusted_revenue"),
    )
)
