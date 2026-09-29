from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Tokyo", "Singapore"]))
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
    )
    .filter(F.col("central_count") >= 68)
    .orderBy("region", "opened_month")
    .limit(5)
)
