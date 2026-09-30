from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy(F.col("first_delivered_date").desc())
)
