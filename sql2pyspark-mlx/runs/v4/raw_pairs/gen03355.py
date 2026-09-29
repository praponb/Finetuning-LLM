from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key")
w_grp = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w4 = Window.orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("rn", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("attr_key").orderBy("corr_latency_ms_status_code", "id", "attr_key")
w_hist = Window.partitionBy("attr_key").orderBy(F.col("first_known_latency_ms").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

deduped = (
    exploded
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w2))
    .withColumn("next_status_code", F.lead("status_code", 1).over(w_grp))
    .withColumn("latency_ms_bucket", F.ntile(10).over(w4))
    .withColumn("last_known_attr_key", F.last("attr_key", ignorenulls=True).over(w5))
    .withColumn("first_known_latency_ms", F.first("latency_ms", ignorenulls=True).over(w6))
)

scored_app_logs = (
    scored
    .withColumn("corr_latency_ms_status_code_2", F.corr("latency_ms", "status_code").over(w2))
    .withColumn("next_next_status_code", F.lead("next_status_code", 1).over(w_ordered))
    .withColumn("first_known_latency_ms_cume_dist", F.cume_dist().over(w_hist))
)

result = (
    scored_app_logs
    .select(
        "id",
        "attr_key",
        "value",
        "rn",
        "next_status_code",
        "last_known_attr_key",
        "first_known_latency_ms",
        "corr_latency_ms_status_code_2",
        "next_next_status_code",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "PRIORITY").otherwise("CORE").alias("attr_key_zone"),
    )
    .orderBy(F.col("next_status_code").asc_nulls_first())
)
