from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.sale_date").between("2023-07-01", "2023-11-01"))
    .groupBy(F.col("s.city"))
    .agg(F.count(F.col("ss.units")).alias("units_count"))
    .filter(F.col("units_count") > 1)
    .orderBy(F.col("units_count").desc(), F.col("city").desc())
)
