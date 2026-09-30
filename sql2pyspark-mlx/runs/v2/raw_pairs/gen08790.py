from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-09-07")
    .select(
        "id",
        "carrier",
        F.concat("status", F.lit(" "), "carrier").alias("status_carrier"),
    )
    .orderBy(F.col("id").desc())
)
