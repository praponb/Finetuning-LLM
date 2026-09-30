from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isin(["North", "Central"]))
    .withColumn("region_group", F.when(F.col("region").isin(["South", "North"]), "Tier 1").otherwise("Other"))
)

summary = (
    base
    .groupBy("region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    summary
    .filter(F.col("stddev_sqft") > 75)
    .select("region", "last_opened_date")
    .orderBy("region")
)
