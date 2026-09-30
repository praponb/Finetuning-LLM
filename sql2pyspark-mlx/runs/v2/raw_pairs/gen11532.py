from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
    )
)
