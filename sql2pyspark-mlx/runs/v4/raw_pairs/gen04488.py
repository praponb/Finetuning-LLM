from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("level_length").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key", "level").orderBy("level_length", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("level_length", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "attr_key").orderBy(F.col("level_length_bucket").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select("id", "host", "level", F.explode("context").alias("attr_key", "v"))
)

filtered = (
    flattened
    .filter(~F.col("attr_key").isin(["trace", "region", "version"]))
    .select("id", "level", "attr_key", "v", F.length("level").alias("level_length"))
)

enriched = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w).alias("level_length_bucket"),
        F.sum("level_length").over(w2).alias("cumulative_level_length"),
    )
)

scored = (
    enriched
    .withColumn("running_level_length_bucket", F.sum("level_length_bucket").over(w3))
    .withColumn("level_length_bucket_bucket", F.ntile(3).over(w4))
)

result = (
    scored
    .select(
        "id",
        "level",
        "attr_key",
        "v",
        "level_length",
        "level_length_bucket",
        "cumulative_level_length",
        "level_length_bucket_bucket",
    )
    .orderBy(F.col("level").asc_nulls_first())
)
