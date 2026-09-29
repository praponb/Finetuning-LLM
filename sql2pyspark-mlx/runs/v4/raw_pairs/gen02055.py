from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2025-01-01", "2025-08-01") & F.col("order_id").isNull() & F.col("order_id").isNotNull())
    .select(
        "status",
        F.add_months("delivered_date", 6).alias("delivered_date_plus_months"),
    )
)
