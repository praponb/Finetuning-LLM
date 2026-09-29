from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 74)
    .select(
        "id",
        "carrier",
        "status",
        "delivered_date",
        F.when(F.col("status") == "DELIVERED", 1).otherwise(0).alias("is_delivered"),
        F.when(F.col("status") == "RETURNED", 1).otherwise(0).alias("is_returned"),
    )
    .orderBy(F.col("is_delivered").desc())
)
