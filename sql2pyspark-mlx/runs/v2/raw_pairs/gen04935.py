from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-10-23", "2024-05-01") & F.col("delivered_date").between("2023-11-28", "2025-07-01"))
)

summary = (
    base
    .groupBy(F.month("delivered_date").alias("delivered_month"), "carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    summary
    .filter(F.col("avg_cost") < 89)
    .select("delivered_month", "carrier", "carrier_values", "avg_cost")
    .orderBy("carrier_values")
)
