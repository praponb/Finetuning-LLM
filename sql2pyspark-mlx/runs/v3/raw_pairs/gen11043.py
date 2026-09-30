from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select("country")
    .intersect(df_sessions.select("country"))
    .orderBy(F.col("country").desc())
)
