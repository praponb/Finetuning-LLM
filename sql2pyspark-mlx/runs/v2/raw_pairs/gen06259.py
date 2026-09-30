from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["RETURNED", "LOST", "DELIVERED"]))
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("pct_kerry") < 74)
    .select("delivered_quarter", "pct_kerry", "status_values")
    .orderBy(F.col("pct_kerry").desc())
)
