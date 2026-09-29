from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").asc_nulls_first(), "id")

deduped = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(F.when(F.col("cost") >= 89, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("cost_per_returned"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "FedEx", 1)), F.lit(0)), 2).alias("cost_per_fedex"),
    )
    .filter(F.col("num_carriers") > 1)
    .orderBy(F.col("cost_tier").asc_nulls_last())
)
