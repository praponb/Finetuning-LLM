from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.0, 3.4))
    .select("supplier_name")
    .unionByName(df_suppliers.filter(F.col("rating") > 2.8).select("supplier_name"))
    .distinct()
)
