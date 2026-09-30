from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") > 87) & (F.col("delivered_date") < "2024-03-01") & F.month("delivered_date").isin([3, 6, 10]))
    .select(
        "delivered_date",
        "cost",
        "carrier",
        F.upper("status").alias("status_upper"),
    )
)
