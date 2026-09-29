from pyspark.sql import functions as F

base = (
    df_orders
    .filter(((F.col("status") != "PENDING") & F.col("store_id").isin([13, 29])) | F.col("store_id").isNotNull())
)

status_summary = (
    base
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.countDistinct("shipping_fee").alias("unique_statuss"),
    )
)

result = (
    status_summary
    .select("status", "returned_count")
    .orderBy("returned_count")
)
