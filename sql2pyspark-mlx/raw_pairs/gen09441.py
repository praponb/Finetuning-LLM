from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .select("supplier_name")
    .unionByName(df_suppliers.filter(F.col("country") != "SG").select("supplier_name"))
    .distinct()
    .orderBy("supplier_name")
)
