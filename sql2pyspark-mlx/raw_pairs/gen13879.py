from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        F.when(F.col("sqft") >= 17465, "high").when(F.col("sqft") >= 2826, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .filter(F.col("total_sqft") > 71000)
    .select("opened_month", "sqft_bucket", "total_sqft")
    .orderBy(F.col("total_sqft").desc(), "opened_month", "sqft_bucket")
    .limit(1)
)
