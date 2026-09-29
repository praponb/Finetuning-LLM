from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") != "DELIVERED") & F.col("cost").between(37, 44))
    .select(
        "order_id",
        "delivered_date",
        F.when(F.col("cost") >= 100, "High").when(F.col("cost") >= 35, "Medium").otherwise("Low").alias("cost_level"),
    )
)
