from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([3, 24, 38]) & F.col("order_id").isin([2, 22, 38]) & F.month("delivered_date").isin([5, 7, 9]))
    .select(
        "id",
        "delivered_date",
        "status",
        "cost",
        F.length("carrier").alias("carrier_length"),
        F.length("status").alias("status_length"),
    )
    .orderBy(F.col("cost").desc(), "id")
    .limit(1)
)
