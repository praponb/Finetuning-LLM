from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("city").isin(["New York", "London", "Bangkok"]))

region_summary = (
    recent_stores
    .groupBy("region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
)

result = (
    region_summary
    .filter(F.col("pct_berlin") > 23)
    .select("region", "pct_berlin", "central_count")
)
