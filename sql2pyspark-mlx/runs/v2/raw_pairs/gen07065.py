from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Flash"]) | F.col("cost").between(71, 72))
    .select(
        "carrier",
        F.date_trunc("YEAR", "shipped_date").alias("shipped_date_period"),
    )
)
