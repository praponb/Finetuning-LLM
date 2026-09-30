from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select("city")
    .intersect(df_suppliers.filter(F.col("rating") < 2.3).select("city"))
    .orderBy("city")
)
