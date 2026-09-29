from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2025)
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy("status")
)
