from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "IN_TRANSIT"]))
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"))
    .filter(F.col("returned_count") < 7)
    .orderBy(F.col("returned_count").desc(), "delivered_month")
    .limit(5)
)
