from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-06-05", "2025-06-01") & F.col("status").isin(["IN_TRANSIT", "DELIVERED", "RETURNED"]) & (F.col("cost") <= 61))
    .groupBy("status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") < 50)
    .drop("min_cost")
    .orderBy("status")
)
