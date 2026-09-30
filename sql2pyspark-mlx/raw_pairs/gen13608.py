from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "South", "East"]))
    .groupBy(
        "city",
        F.when(F.col("region").isin(["East", "South"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
)
