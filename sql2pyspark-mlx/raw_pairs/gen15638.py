from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 29) & ((F.col("cost") <= 68) | F.col("status").isin(["DELIVERED", "RETURNED", "IN_TRANSIT"])))
    .groupBy("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
    .orderBy(F.col("dhl_cost").desc(), F.col("row_count").desc())
)
