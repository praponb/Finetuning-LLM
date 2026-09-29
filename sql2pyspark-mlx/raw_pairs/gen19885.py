from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "RETURNED") & (F.col("order_id").isNotNull() | F.col("order_id").isNotNull()))
    .groupBy(
        F.when(F.col("carrier").isin(["Kerry", "UPS"]), "priority").otherwise("other").alias("carrier_group"),
        F.when(F.col("cost") >= 100, "high").when(F.col("cost") >= 35, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.max("shipped_date").alias("last_shipped_date"),
    )
    .orderBy(F.col("last_shipped_date").desc())
)
