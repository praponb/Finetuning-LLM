from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull() | F.col("cost").between(33, 73))
    .select(
        "id",
        "status",
        "order_id",
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
    )
)
