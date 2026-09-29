from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .select(
        "id",
        "shipped_date",
        "carrier",
        "status",
        F.date_format("shipped_date", "MMM yyyy").alias("shipped_date_label"),
    )
    .orderBy(F.col("shipped_date_label").desc(), "id")
    .limit(3)
)
