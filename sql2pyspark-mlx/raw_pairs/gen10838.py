from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.max("cost").alias("max_cost"),
    )
)
