from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"))
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.countDistinct("carrier").alias("distinct_carriers"),
    )
)
