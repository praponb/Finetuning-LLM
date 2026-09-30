from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.max("sqft").alias("max_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.sum("sqft").alias("total_sqft"),
    )
)
