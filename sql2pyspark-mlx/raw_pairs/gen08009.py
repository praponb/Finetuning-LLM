from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc_nulls_first())
w2 = Window.partitionBy("year_start").orderBy(F.col("returned_events").desc())

latest = (
    df_shipments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("year", "delivered_date").alias("year_start"),
        F.when(F.col("cost") >= 105, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.min("cost").alias("min_cost"),
    )
    .withColumn("returned_events_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("row_count").desc())
)
