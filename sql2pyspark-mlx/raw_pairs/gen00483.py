from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "Berlin"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(F.avg("sqft").alias("avg_sqft"))
    .filter(F.col("avg_sqft") > 15200)
    .select("opened_month", "region", "avg_sqft")
    .orderBy(F.col("opened_month").desc())
)
