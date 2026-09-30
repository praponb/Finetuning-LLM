from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["IN_TRANSIT", "RETURNED", "LOST"]) & (F.col("status") == "LOST") & F.col("order_id").isin([16, 17, 34]))
    .groupBy("order_id", F.year("delivered_date").alias("delivered_year"))
    .agg(F.count("cost").alias("cost_count"))
    .orderBy(F.col("order_id").desc(), "cost_count", "delivered_year")
    .limit(3)
)
