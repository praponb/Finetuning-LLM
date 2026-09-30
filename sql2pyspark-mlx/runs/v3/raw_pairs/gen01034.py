from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "Kerry", "DHL"]) & F.col("shipped_date").between("2025-06-03", "2025-08-01"))
)

delivered_year_summary = (
    base
    .groupBy(F.year("delivered_date").alias("delivered_year"), "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = delivered_year_summary
