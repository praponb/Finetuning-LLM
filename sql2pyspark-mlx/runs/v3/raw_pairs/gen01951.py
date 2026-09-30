from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(~((F.year("end_date") >= 2025) & (F.col("customer_id").isin([1, 12]) | (F.year("start_date") == 2025))))
)

summary = (
    src
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
)

result = summary.select("customer_id", "cnt", "pct_premium")
