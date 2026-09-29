from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2025-01-14")
    .groupBy("carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("pct_fedex") >= 32)
    .select("carrier", "pct_fedex", "stddev_cost")
    .orderBy(F.col("pct_fedex").desc(), "carrier")
)
