from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "UPS") & (F.month("shipped_date").isin([3, 8, 12]) | (F.col("delivered_date") > "2025-03-01")))
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
    )
    .filter(F.col("in_transit_count") > 26)
    .drop("in_transit_count")
)
