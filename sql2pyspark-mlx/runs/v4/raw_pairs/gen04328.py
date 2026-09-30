from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("discount_per_returned"),
    )
    .orderBy(F.col("channel").desc())
)
