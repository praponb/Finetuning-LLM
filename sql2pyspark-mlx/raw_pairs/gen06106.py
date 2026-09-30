from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(~(F.col("ss.store_id").isin([1, 4, 5]) & F.col("ss.store_id").isin([1, 2, 3, 6])))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("p.price"),
    )
)
