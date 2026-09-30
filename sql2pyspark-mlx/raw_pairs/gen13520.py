from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "FedEx") | (F.col("shipped_date") > "2023-10-01"))
    .select(
        "id",
        "shipped_date",
        "carrier",
        "cost",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.round((F.datediff("shipped_date", "delivered_date") / 30), 1).alias("months_delivered_date_to_shipped_date"),
    )
)
