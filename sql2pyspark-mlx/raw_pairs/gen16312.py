from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2025-02-01")
    .select(
        "id",
        "carrier",
        "status",
        "shipped_date",
        F.round((F.col("cost") * 0.9), 2).alias("cost_adjusted"),
    )
    .orderBy(F.col("id").desc())
)
