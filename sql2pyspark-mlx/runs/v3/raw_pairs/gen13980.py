from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("cost")).otherwise(0)).alias("initech_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("S.stock_qty_range"), F.col("S.initech_cost"))
)
