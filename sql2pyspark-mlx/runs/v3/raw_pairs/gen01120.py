from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2023-10-01") & F.col("order_id").isNotNull() & F.col("delivered_date").between("2024-05-31", "2025-02-01"))
    .select(
        "id",
        "carrier",
        "order_id",
        "cost",
        F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"),
    )
    .orderBy("carrier")
)
