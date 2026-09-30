from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("rating") > 2.2)

suppliers_stats = (
    base
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("pct_berlin") > 45)
    .orderBy("country")
)
