from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "shipped_date",
        F.substring("carrier", 1, 2).alias("carrier_prefix"),
        F.trim("carrier").alias("clean_carrier"),
    )
    .orderBy("clean_carrier")
)
