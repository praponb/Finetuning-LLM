from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["West", "North"]))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("total_sqft") > 76000)
    .orderBy(F.col("total_sqft").desc())
)
