from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Bangkok") | (F.length("store_name") < 13))
    .groupBy("region")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
)
