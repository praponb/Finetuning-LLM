from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2025)
    .select("id", "shipped_date")
    .orderBy(F.col("shipped_date").desc())
)
