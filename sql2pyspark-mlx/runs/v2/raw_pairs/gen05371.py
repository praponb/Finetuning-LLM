from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([19, 26, 31, 36]))
    .groupBy("status")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
    )
)
