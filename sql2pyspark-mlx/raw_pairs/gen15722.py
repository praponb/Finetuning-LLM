from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 4)
)
