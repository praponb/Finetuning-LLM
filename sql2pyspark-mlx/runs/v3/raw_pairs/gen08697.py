from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 39) & F.col("status").isin(["LOST", "RETURNED"]) & (F.col("status") != "LOST"))
    .select(
        "shipped_date",
        F.when(F.col("carrier").isin(["Flash", "UPS"]), "Core").otherwise("Other").alias("carrier_group"),
    )
    .orderBy(F.col("shipped_date").desc())
)
