from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & (F.col("cost") < 36))
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("min_cost").desc())
)
