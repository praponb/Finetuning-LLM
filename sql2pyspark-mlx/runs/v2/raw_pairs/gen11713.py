from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") >= 2025) & F.col("delivered_date").between("2024-05-21", "2025-02-01") & (F.col("shipped_date") < "2025-08-01"))
    .groupBy("carrier", F.month("shipped_date").alias("shipped_month"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
)
