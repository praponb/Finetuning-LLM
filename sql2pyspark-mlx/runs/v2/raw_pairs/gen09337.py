from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("city").isin(["London", "New York"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
