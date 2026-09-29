from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .intersect(df_sessions.select("country"))
    .orderBy(F.col("country").desc())
)
