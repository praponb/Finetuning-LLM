from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "web") & F.col("channel").isin(["web", "partner"]) & F.col("status").isin(["CANCELLED", "RETURNED"]))
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
        F.min("order_date").alias("first_order_date"),
    )
    .filter(F.col("returned_amount") < 60)
)
