from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST"]) & ((F.year("shipped_date") >= 2025) | F.col("shipped_date").between("2024-02-01", "2024-09-01")))
    .groupBy("order_id", "carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .filter(F.col("returned_count") > 28)
    .drop("returned_count")
)
