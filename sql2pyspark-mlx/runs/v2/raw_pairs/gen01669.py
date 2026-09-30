from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Tokyo")
    .select("city", "country")
    .intersect(df_suppliers.select("city", "country"))
    .orderBy(F.col("country").desc())
)
