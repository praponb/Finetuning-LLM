from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 6)
    .select("channel", "order_quarter", "cnt", "total_count")
)
