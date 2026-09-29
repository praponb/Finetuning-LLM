from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("city").isin(["London", "Tokyo"]))
    .withColumn("region_title", F.initcap(F.trim("region")))
)

stores_stats = (
    recent_stores
    .groupBy(
        "city",
        F.when(F.col("region").isin(["West", "Central"]), "group_a").otherwise("other").alias("region_group"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
    )
)

result = (
    stores_stats
    .select("city", "region_group", "chiang_mai_sqft")
    .orderBy("region_group")
)
