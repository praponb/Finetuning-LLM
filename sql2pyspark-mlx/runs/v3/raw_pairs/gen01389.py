from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
    )
)
