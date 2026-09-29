from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("region").isin(["Central", "South"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .filter(F.col("min_sqft") > 14200)
    .orderBy("region_group")
    .limit(5)
)
