from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.sale_date").between("2023-11-01", "2023-12-04"))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
    .orderBy("price")
)
