from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "carrier",
        "delivered_date",
        "cost",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)
