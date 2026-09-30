from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 63)
    .groupBy("order_id")
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") >= 55)
    .select("order_id", "last_delivered_date", "max_cost")
    .orderBy(F.col("max_cost").desc(), "order_id")
    .limit(20)
)
