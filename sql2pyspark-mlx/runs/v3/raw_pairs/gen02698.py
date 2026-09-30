from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") > "2023-09-01") & (F.col("delivered_date") > "2024-06-01") & (F.col("delivered_date") > "2024-01-01"))
    .select(
        "id",
        "shipped_date",
        "carrier",
        F.floor("cost").alias("cost_whole"),
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
)
