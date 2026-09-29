from pyspark.sql import functions as F

result = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) >= 1), "left_anti")
)
