from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Traders%"))
    .select("id", "city")
    .unionByName(df_suppliers.filter(F.col("country") == "US").select("id", "city"))
    .distinct()
)
