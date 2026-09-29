from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") > "2023-04-10") & ~F.col("status").isin(["CANCELLED", "RETURNED", "COMPLETED"]))
    .groupBy("channel")
    .agg(
        F.max("discount").alias("max_discount"),
        F.count("*").alias("cnt"),
    )
)
