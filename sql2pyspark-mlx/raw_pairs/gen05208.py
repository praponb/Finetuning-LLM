from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-05-01", "2025-05-01"))
    .groupBy(
        F.when(F.col("carrier").isin(["DHL", "FedEx"]), "core").otherwise("other").alias("carrier_group"),
        "status",
    )
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum("cost").alias("total_cost"),
    )
)
