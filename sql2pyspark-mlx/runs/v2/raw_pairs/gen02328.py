from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") >= 2024) & F.col("order_id").isNotNull())
    .select(
        "id",
        "status",
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
    )
)
