from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "DELIVERED") & (F.col("status") == "LOST"))
    .select(
        "id",
        "delivered_date",
        "cost",
        "shipped_date",
        F.date_trunc("YEAR", "shipped_date").alias("shipped_date_period"),
    )
)
