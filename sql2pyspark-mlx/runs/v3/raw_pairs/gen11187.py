from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "cost",
        "delivered_date",
        F.concat("carrier", F.lit(" - "), "status").alias("carrier_status"),
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
)
