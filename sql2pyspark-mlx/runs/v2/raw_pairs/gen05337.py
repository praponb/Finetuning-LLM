from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 57)
    .rollup("carrier", F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("cost").alias("max_cost"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
)
