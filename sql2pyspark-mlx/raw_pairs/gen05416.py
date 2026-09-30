from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select("country")
    .unionByName(df_suppliers.filter(F.length("supplier_name") >= 10).select("country"))
    .distinct()
)
