from pyspark.sql import functions as F

lines = df_app_logs.select("id", "level", F.explode("context").alias("attr_key", "v"))

result = (
    lines
    .select(
        "id",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "GROUP_A").when(F.col("attr_key").rlike("^(version)$"), "OTHER_ZONE").otherwise("CORE").alias("attr_key_zone"),
    )
)
