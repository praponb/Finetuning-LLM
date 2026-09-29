from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]) & F.col("order_id").isNotNull())
)

status_summary = (
    src
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
    )
)

result = (
    status_summary
    .filter(F.col("pct_kerry") < 36)
    .select("status", "carrier_values", "pct_kerry")
)
