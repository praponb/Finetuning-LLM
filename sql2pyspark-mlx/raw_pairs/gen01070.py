from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") != "DELIVERED") | (F.col("cost") > 63))
    .rollup(
        "carrier",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
    )
)
