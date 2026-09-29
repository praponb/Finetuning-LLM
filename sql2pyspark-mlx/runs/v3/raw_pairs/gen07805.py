from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "FedEx") & (F.col("delivered_date").between("2024-12-01", "2025-07-01") | (F.col("shipped_date") > "2024-04-01")))
    .select(
        "status",
        "order_id",
        F.year("shipped_date").alias("shipped_date_year"),
        F.add_months("delivered_date", 12).alias("delivered_date_plus_months"),
    )
)
