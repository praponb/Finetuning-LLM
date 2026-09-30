from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Kerry") & F.col("shipped_date").between("2024-03-01", "2024-10-01"))
    .select(
        "id",
        "shipped_date",
        "status",
        "order_id",
        F.concat("status", F.lit(" - "), "carrier").alias("status_carrier"),
    )
)
