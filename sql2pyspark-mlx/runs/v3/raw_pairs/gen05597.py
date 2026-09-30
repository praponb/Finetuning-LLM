from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .subtract(df_sessions.filter(F.col("device") != "mobile").select("country"))
)
