from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy("status")
)
