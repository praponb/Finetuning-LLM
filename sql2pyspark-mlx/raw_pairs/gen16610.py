from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select("id", "city", "opened_date", F.lower("region").alias("region_lower"))
)

result = (
    filtered
    .select("city", "id")
    .exceptAll(
        df_stores
        .filter(F.col("sqft").between(4914, 15288))
        .select("city", "id")
    )
)
