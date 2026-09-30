from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(82, 90) | F.col("status").isin(["LOST", "IN_TRANSIT", "DELIVERED"]))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
)
