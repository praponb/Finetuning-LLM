from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-08-18", "2024-11-01") & (F.col("shipped_date") >= "2023-09-01") & F.col("order_id").isin([7, 17, 19, 34]))
    .groupBy("status")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 54)
    .orderBy("cost_range", "status")
    .limit(5)
)
