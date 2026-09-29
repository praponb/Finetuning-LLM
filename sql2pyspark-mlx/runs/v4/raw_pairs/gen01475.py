from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1480)
    .groupBy("channel", F.year("order_date").alias("order_year"))
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
    .filter(F.col("returned_count") > 7)
    .orderBy("channel")
)
