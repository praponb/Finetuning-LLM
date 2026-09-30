from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.sum("cost").alias("total_cost"),
        F.min("cost").alias("min_cost"),
    )
)
