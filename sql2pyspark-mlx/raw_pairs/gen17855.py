from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("city") != "Chiang Mai") & F.col("opened_date").between("2024-10-01", "2025-09-01")))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
