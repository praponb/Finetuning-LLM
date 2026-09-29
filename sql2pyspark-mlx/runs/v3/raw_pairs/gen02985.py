from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2025-07-01")
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"), "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy(F.col("status").desc(), "shipped_month")
    .limit(20)
)
