from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select(
        "id",
        "cost",
        "shipped_date",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.upper("carrier").alias("carrier_upper"),
    )
)
