from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select("country")
    .unionByName(df_suppliers.filter(F.col("rating") > 3.3).select("country"))
    .distinct()
    .orderBy("country")
)
