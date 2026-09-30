from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_date_label"),
    )
)
