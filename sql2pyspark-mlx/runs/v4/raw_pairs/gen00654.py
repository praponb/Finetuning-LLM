from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("carrier").isin(["UPS", "DHL"]), "Group A").otherwise("Other").alias("carrier_group"),
        F.when(F.col("status") == "DELIVERED", 1).otherwise(0).alias("is_delivered"),
    )
)

result = (
    base
    .select("status")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date") > "2023-07-01")
        .select("status")
    )
)
