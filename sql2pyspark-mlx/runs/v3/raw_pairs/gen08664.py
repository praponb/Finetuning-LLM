from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.store_id").isin([2, 3, 4, 6]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.cost"),
    )
    .orderBy(F.col("sale_date").desc(), F.col("cost").desc(), "id")
    .limit(10)
)
