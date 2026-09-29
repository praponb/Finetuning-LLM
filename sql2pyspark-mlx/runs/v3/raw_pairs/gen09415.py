from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-08-15")
    .select(
        "carrier",
        "cost",
        "status",
        F.when(F.col("carrier") == "UPS", 1).otherwise(0).alias("is_ups"),
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)
