from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .groupBy("order_id")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
    .filter(F.col("pct_in_transit") >= 32)
)
