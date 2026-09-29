from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-03-01", "2024-09-30") & F.col("carrier").isin(["DHL", "FedEx"]))
    .select(
        "shipped_date",
        "cost",
        "delivered_date",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        F.lower("carrier").alias("carrier_lower"),
    )
    .orderBy("carrier_title", "carrier_lower")
)
