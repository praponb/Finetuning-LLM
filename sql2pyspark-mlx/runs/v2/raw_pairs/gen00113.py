from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Ri%") & F.col("opened_date").between("2023-03-20", "2024-03-01"))
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .orderBy(F.col("bangkok_count").desc())
)
