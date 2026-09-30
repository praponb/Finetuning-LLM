from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") != "FedEx") & (F.col("delivered_date") > "2024-12-01"))
    .select(
        "id",
        "delivered_date",
        "carrier",
        F.date_sub("shipped_date", 76).alias("shipped_date_minus_76d"),
    )
)
