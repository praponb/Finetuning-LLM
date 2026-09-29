from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("cost_rounded_tier").orderBy(F.col("distinct_carriers").desc())

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-03-06").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        F.round("cost", 0).alias("cost_rounded"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("cost_rounded") >= 66, "HIGH").otherwise("LOW").alias("cost_rounded_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("cost_rounded", 0.9).alias("cost_rounded_p90"),
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
        F.count("*").alias("cnt"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.count("*").alias("row_count"),
    )
    .withColumn("distinct_carriers_rank", F.rank().over(w))
    .orderBy(F.col("cost_rounded_tier").asc_nulls_first())
)
