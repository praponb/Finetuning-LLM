from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") >= 12)
    .groupBy(
        "segment",
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "priority").otherwise("other").alias("segment_group"),
    )
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
    .filter(F.col("stddev_age") < 40)
    .select("segment", "segment_group", "stddev_age")
    .orderBy(F.col("stddev_age").desc(), F.col("segment_group").desc(), "segment")
    .limit(20)
)
