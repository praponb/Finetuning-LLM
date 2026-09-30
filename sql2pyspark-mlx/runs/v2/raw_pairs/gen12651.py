from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.price").between(580, 1170) & F.month(F.col("ss.sale_date")).isin([6, 7, 8]))
    .select(
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("ss.units"),
        F.col("p.cost").cast("int").alias("cost_int"),
    )
    .orderBy(F.col("units").desc(), F.col("cost_int").desc())
)
