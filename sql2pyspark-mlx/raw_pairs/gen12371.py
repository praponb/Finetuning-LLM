from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["LOST", "RETURNED", "IN_TRANSIT"]))
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "LOST"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.max("cost").alias("max_cost"),
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
)
