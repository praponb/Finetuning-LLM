from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~(F.col("supplier_name").like("%Gmb%") & F.col("city").isin(["London", "Chiang Mai", "Singapore"]) & ~F.col("city").isin(["Singapore", "Chiang Mai"])))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "SG", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sg"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy("city")
)
