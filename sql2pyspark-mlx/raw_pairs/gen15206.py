from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .unionByName(df_sessions.filter(F.col("device") != "mobile").select("country"))
    .distinct()
)
