from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-04-30", "2025-08-01") & F.col("order_id").isNull())
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .orderBy(F.col("status").desc())
)
