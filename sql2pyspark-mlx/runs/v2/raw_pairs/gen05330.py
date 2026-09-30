from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-11-01", "2025-05-01") & ((F.year("delivered_date") == 2023) | (F.col("shipped_date") > "2025-09-13")))
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.trim("carrier").alias("clean_carrier"),
        F.concat("status", F.lit(", "), "carrier").alias("status_carrier"),
    )
)
