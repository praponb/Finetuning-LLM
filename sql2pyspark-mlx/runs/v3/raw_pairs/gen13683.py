from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
    )
    .orderBy(F.col("pct_kerry").desc())
)
