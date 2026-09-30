from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "TH", "DE"]))
    .select("city")
    .unionByName(df_suppliers.filter(F.col("rating") >= 3.0).select("city"))
    .distinct()
)
