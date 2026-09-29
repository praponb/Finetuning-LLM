from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") >= 2024)
    .groupBy(
        F.when(F.col("region").isin(["North", "West"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)
