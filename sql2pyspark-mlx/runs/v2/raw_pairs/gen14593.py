from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("region") != "East") & F.col("opened_date").between("2024-10-01", "2025-06-01"))
)

opened_month_summary = (
    base
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "city")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
    )
)

result = (
    opened_month_summary
    .filter(F.col("tokyo_count") < 79)
    .orderBy("tokyo_count", "city", "opened_month")
    .limit(10)
)
