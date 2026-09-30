from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "IN_TRANSIT", "RETURNED"]))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
    )
)
