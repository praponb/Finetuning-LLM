from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 50)
    .select(
        "id",
        "cost",
        "order_id",
        F.trim("carrier").alias("clean_carrier"),
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
    .orderBy("cost")
)
