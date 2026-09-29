from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 4.1)
    .select("country")
    .subtract(df_sessions.select("country"))
)
