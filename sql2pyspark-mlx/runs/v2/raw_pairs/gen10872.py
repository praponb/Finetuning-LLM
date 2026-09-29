from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty").isNull())
    .groupBy("supplier_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.city"), F.col("pbs.brand_values"))
    .orderBy(F.col("brand_values").desc())
)
