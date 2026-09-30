from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 88)
    .groupBy(
        "status",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)
