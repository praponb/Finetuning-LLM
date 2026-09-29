from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "RETURNED")
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
    .filter(F.col("flash_count") > 28)
    .select("order_id", "stddev_cost", "flash_count")
    .orderBy(F.col("stddev_cost").desc())
)
