from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("S%"))
    .select("supplier_name")
    .intersect(df_suppliers.filter(F.col("rating") > 1.8).select("supplier_name"))
    .orderBy("supplier_name")
)
