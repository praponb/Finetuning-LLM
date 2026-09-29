from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
    )
)
