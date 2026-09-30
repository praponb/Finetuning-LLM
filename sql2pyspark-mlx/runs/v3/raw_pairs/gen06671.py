from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "siam parts co")
    .select("id", "country")
    .unionByName(df_suppliers.filter(F.col("rating") >= 3.6).select("id", "country"))
    .distinct()
)
