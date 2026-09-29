from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("brand") != "Globex").alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("ss.store_id").isin([3, 4, 5]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("ss.revenue"),
    )
)
