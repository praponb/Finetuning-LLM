from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .unionByName(df_sessions.select("country"))
)
