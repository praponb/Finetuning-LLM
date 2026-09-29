from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("launch_date").between("2023-03-01", "2024-10-31"))
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("pbs.pct_initech"),
        F.col("pbs.max_stock_qty"),
    )
    .orderBy(F.col("pct_initech").desc(), F.col("max_stock_qty").desc(), "id")
    .limit(20)
)
