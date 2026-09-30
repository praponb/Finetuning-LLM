from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.revenue") > 2800)
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("p.cost"),
        F.col("p.cost").cast("int").alias("cost_int"),
    )
    .orderBy("product_id")
)
