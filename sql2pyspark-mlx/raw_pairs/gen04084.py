from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")

latest = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(F.when(F.col("cost") >= 78, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.min("cost").alias("min_cost"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
    )
    .filter(F.col("min_cost") >= 0)
    .orderBy(F.col("cost_stddev").asc_nulls_first())
)
