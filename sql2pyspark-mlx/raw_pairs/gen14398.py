from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("region").isin(["East", "South"]), "group_a").otherwise("other").alias("region_group"),
    )
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
)
