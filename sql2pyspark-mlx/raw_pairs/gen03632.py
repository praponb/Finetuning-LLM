from pyspark.sql import functions as F

result = (
    df_orders
    .rollup("status")
    .agg(
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
