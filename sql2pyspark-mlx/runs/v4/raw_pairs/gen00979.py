from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("brand").isin(["Initech", "Umbrella"])).alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("ss.sale_date").between("2023-08-01", "2024-07-26"))
    .select(F.col("ss.id"), F.col("ss.product_id"))
)
