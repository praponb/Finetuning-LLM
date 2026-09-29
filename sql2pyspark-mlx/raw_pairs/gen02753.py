from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Singapore", "Chiang Mai"])
    .agg(F.max("sqft").alias("total"))
    .select(
        "region",
        F.col("Singapore").alias("singapore"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
