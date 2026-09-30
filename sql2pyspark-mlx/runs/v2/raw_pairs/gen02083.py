from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") | (F.col("status") != "LOST"))
    .select(
        "id",
        "order_id",
        "delivered_date",
        F.when(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]), "Group A").otherwise("Other").alias("status_group"),
    )
    .orderBy("id", "delivered_date")
)
