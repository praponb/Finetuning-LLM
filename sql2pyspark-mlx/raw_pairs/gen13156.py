from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_last(), "id")

latest = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        "status",
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
        F.when(F.col("cost") >= 104, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "DHL", 1)), F.lit(0)), 2).alias("cost_per_dhl"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.stddev_samp("cost").alias("cost_stddev_2"),
    )
)
