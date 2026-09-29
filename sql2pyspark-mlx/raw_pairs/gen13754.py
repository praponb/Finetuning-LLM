from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 700) & (F.col("status") == "CANCELLED"))
    .select(
        "id",
        "shipping_fee",
        "store_id",
        "status",
        F.when(F.col("status").isin(["RETURNED", "PENDING"]), "Group A").otherwise("Other").alias("status_group"),
    )
)
