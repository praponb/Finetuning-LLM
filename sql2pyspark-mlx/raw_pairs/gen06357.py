from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") > "2025-07-01") | F.col("carrier").isin(["Flash", "Kerry", "UPS"]))
    .groupBy(
        "carrier",
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("cost").alias("cost_count"),
    )
)
