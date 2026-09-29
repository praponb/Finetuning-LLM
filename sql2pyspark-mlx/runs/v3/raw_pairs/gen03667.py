from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Who%"))
    .select("id", "country")
    .unionByName(df_suppliers.filter(F.col("city") == "Tokyo").select("id", "country"))
    .distinct()
    .orderBy("id")
)
