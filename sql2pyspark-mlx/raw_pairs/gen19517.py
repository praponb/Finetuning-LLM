from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") > 64)
)
