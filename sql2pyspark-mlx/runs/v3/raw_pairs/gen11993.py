from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Kerry") & (F.col("status") == "RETURNED") & F.col("carrier").isin(["FedEx", "Flash", "UPS"]))
    .groupBy("status", F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .filter(F.col("pct_returned") >= 51)
    .orderBy(F.col("flash_count").desc(), F.col("pct_returned").desc())
)
