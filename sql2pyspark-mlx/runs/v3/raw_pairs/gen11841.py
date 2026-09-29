from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "shipped_date",
        F.when(F.col("carrier").isin(["UPS", "FedEx"]), "Core").otherwise("Other").alias("carrier_group"),
        F.date_sub("delivered_date", 43).alias("delivered_date_minus_43d"),
    )
)
