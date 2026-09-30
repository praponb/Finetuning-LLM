from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 66) | (F.col("shipped_date") < "2023-06-01"))
    .select(
        "status",
        "carrier",
        "shipped_date",
        F.round("cost", 1).alias("cost_rounded"),
    )
)
