from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        "carrier",
        "cost",
        (F.datediff(F.current_date(), "delivered_date") / 365).alias("years_since_delivered_date"),
        F.substring("carrier", 1, 3).alias("carrier_prefix"),
    )
)
