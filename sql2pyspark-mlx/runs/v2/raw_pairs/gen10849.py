from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([12, 17]) & (F.col("status") != "LOST"))
    .groupBy("status", "carrier", F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .orderBy("first_delivered_date")
)
