from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-09-14", "2024-05-04") & F.col("store_name").like("R%"))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
    )
    .filter(F.col("bangkok_sqft") > 43)
    .orderBy("city", "opened_quarter")
    .limit(5)
)
