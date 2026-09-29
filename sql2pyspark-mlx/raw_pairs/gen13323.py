from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "FedEx"]))
    .select(
        "id",
        "delivered_date",
        "status",
        "cost",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
    )
)
