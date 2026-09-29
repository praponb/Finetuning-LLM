from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["New York", "Bangkok"])
    .agg(F.max("sqft").alias("total"))
    .select(
        "region",
        F.col("New York").alias("new_york"),
        F.col("Bangkok").alias("bangkok"),
    )
)
