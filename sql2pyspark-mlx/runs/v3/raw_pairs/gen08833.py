from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select("id", "city")
    .unionByName(df_suppliers.filter(F.col("city") == "Berlin").select("id", "city"))
    .distinct()
    .orderBy("city")
)
