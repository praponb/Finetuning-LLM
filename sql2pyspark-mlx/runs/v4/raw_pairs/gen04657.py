from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eu%"))
    .select("city", "country")
    .unionByName(df_suppliers.filter(F.col("rating") >= 3.9).select("city", "country"))
    .distinct()
)
