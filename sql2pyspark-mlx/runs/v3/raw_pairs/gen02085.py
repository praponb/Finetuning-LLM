from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 4, 12]))
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
)
