from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "cost",
        F.date_trunc("WEEK", "delivered_date").alias("delivered_date_period"),
    )
)
