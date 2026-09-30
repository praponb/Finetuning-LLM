from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "status_code", F.explode_outer("context").alias("attr_key", "v"))
)

result = (
    exploded
    .groupBy(
        "attr_key",
        F.when(F.col("status_code") >= 403, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
    )
    .orderBy(F.col("status_code_tier").asc_nulls_last())
)
