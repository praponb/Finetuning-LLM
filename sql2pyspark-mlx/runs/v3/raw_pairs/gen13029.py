from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "FedEx", "DHL"]) & F.col("cost").between(84, 85))
    .groupBy(
        F.when(F.col("status").isin(["IN_TRANSIT", "LOST"]), "group_a").otherwise("other").alias("status_group"),
        "carrier",
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
    )
    .filter(F.col("cost_count") >= 2)
    .orderBy(F.col("returned_cost").desc())
)
