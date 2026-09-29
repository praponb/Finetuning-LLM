from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("customer_id").isin([14, 24, 34]) & ~F.col("status").isin(["CANCELLED", "EXPIRED", "ACTIVE"])))
    .select("id", "start_date", F.year("start_date").alias("start_date_year"))
    .orderBy(F.col("id").desc(), "start_date_year")
)
