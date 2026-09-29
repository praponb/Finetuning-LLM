from pyspark.sql import functions as F

base = df_stores.filter(F.col("sqft") == 7452)

summary = (
    base
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
    )
)

result = (
    summary
    .select("opened_quarter", "region", "avg_sqft", "south_sqft")
    .orderBy(F.col("avg_sqft").desc(), "opened_quarter", "region")
    .limit(3)
)
