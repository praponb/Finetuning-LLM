from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["East", "Central"]))
    .groupBy(
        F.when(F.col("sqft") >= 13712, "high").when(F.col("sqft") >= 2451, "mid").otherwise("low").alias("sqft_bucket"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)
