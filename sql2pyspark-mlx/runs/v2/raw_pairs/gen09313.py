from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("delivered_date") < "2025-09-25") & F.col("carrier").isin(["UPS", "Flash", "Kerry"]))
)

agg = (
    valid_shipments
    .groupBy("carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
)

result = agg.orderBy(F.col("pct_in_transit").desc())
