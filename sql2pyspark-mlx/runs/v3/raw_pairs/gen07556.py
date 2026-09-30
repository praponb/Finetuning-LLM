from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-07-19", "2024-08-01"))
    .groupBy("carrier")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.sum("cost").alias("total_cost"),
    )
)
