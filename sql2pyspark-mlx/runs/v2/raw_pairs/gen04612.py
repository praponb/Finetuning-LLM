from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["RETURNED", "LOST"]))
    .select(
        "order_id",
        "status",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
)
