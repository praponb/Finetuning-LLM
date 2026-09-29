from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
