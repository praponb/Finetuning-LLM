from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 91) & (F.col("delivered_date") > "2023-03-08"))
    .groupBy("status")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy("avg_cost")
)
