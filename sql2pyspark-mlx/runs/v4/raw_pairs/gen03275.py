from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([7, 21, 27, 40]))
    .groupBy("status")
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("min_cost") > 52)
    .select("status", "min_cost", "cnt")
)
