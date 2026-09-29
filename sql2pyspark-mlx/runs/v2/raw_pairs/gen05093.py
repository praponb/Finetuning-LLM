from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNull())
    .groupBy("channel")
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
    )
)
