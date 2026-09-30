from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .intersect(df_sessions.filter(F.col("device") == "tablet").select("country"))
    .orderBy("country")
)
