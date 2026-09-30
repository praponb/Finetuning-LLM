from pyspark.sql import functions as F

base = df_stores.filter(F.col("opened_date") <= "2025-05-21")

summary = (
    base
    .groupBy("region")
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
)

result = (
    summary
    .orderBy("avg_sqft", "region")
    .limit(3)
)
