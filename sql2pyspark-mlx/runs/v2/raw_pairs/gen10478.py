from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([9, 10, 11]) & (F.col("carrier") != "FedEx"))
    .select(
        "carrier",
        F.date_format("delivered_date", "MMM yyyy").alias("delivered_date_label"),
    )
    .orderBy(F.col("carrier").desc())
)
