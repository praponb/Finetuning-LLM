from pyspark.sql import functions as F

result = (
    df_shipments
    .groupingSets([["order_id", "status"], ["order_id"]], "order_id", "status")
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "DHL", 1)), F.lit(0)), 2).alias("cost_per_dhl"),
    )
    .filter(F.col("cost_p95") > 0)
    .orderBy(F.col("status").desc())
)
