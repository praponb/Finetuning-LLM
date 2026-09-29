from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("cost_range") > 60)
    .orderBy("status", F.col("cost_range").desc())
    .limit(3)
)
