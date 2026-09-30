from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("status").isin(["LOST", "RETURNED"]) | (F.col("delivered_date") <= "2025-03-01")))
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"))
    .orderBy("dhl_count")
)
