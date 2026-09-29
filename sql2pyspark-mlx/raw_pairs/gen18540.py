from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-06-02").cast("date"))
    .select(
        "*",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
    )
)

result = (
    valid
    .groupBy("carrier")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_per_delivered"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
)
