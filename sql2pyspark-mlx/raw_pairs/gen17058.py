from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("store_name").like("%Expr%") | ~F.col("region").isin(["North", "South"]))
)

summary = (
    recent_stores
    .groupBy("city", "region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "North", 1).otherwise(0))) / F.count("*")), 1).alias("pct_north"),
    )
)

result = (
    summary
    .select("city", "region", "avg_sqft")
    .orderBy("city")
)
