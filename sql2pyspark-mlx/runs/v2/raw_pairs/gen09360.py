from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "IN_TRANSIT") & F.col("status").isin(["RETURNED", "DELIVERED", "LOST"]) & (F.col("order_id") == 23))
    .select(
        "id",
        "status",
        "shipped_date",
        "order_id",
        F.date_trunc("year", "shipped_date").alias("shipped_date_period"),
        F.when(F.col("carrier").isin(["Kerry", "FedEx"]), "Priority").otherwise("Other").alias("carrier_group"),
    )
)
