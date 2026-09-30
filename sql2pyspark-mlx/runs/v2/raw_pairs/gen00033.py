from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        F.add_months("delivered_date", 6).alias("delivered_date_plus_months"),
    )
)
