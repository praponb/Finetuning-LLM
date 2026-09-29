from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.revenue").between(2300, 3800))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.country"),
    )
    .orderBy("category")
)
