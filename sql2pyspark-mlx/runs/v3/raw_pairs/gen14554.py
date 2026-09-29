from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .filter((F.year(F.col("p.launch_date")) == 2025) | (F.col("p.cost") <= 470))
    .select(
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("p.price"),
        F.col("p.launch_date"),
    )
    .orderBy("price")
)
