from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Chiang Mai") & F.col("region").isin(["South", "North", "West"]) & F.col("opened_date").between("2024-09-27", "2025-01-01"))
    .select("id", "opened_date", "city", "sqft")
)
