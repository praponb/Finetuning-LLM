from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("delivered_date") == 2023) & F.col("order_id").isNull())
    .groupBy("carrier", "order_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("avg_cost") < 54)
    .orderBy("order_id")
)
