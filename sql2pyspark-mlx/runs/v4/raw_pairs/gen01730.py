from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") <= "2024-02-01") | F.col("order_id").isin([6, 28]))
    .groupBy("status", "carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
    .filter(F.col("kerry_count") < 60)
    .drop("kerry_count")
)
