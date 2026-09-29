from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([7, 35]))
    .groupBy("carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)
