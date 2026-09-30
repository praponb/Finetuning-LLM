from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["US", "JP", "DE"]))
    .select("country")
    .intersect(df_sessions.select("country"))
)
