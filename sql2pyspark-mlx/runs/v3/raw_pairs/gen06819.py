from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("sqft_range") > 73)
)
