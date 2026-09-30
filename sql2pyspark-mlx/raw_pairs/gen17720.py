from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("region").isin(["South", "North"]), "group_a").otherwise("other").alias("region_group"),
        "region",
    )
    .agg(
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
    )
    .filter(F.col("south_sqft") < 62)
    .orderBy("region")
)
