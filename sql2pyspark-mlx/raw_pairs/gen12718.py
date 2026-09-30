from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
        F.when(F.col("cost") >= 81, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.stddev_samp("cost").alias("cost_stddev_2"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
