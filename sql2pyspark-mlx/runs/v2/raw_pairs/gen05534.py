from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "London", "Tokyo"]))
    .select("country")
    .intersect(df_sessions.select("country"))
)
