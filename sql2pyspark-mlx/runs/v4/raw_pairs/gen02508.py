from pyspark.sql import functions as F

exploded = df_app_logs.select("id", F.explode("context").alias("k", "value"))

base = (
    exploded
    .filter(F.col("k").isin(["version", "region"]))
    .withColumn("k_filled", F.coalesce("k", F.lit("unassigned")))
    .withColumn("k_title", F.initcap(F.trim("k")))
    .withColumn("k_clean", F.coalesce("k", F.lit("OTHER")))
)

filtered = (
    base
    .filter(F.col("k").isin(["region", "version"]))
    .select(
        "id",
        "k",
        "value",
        "k_title",
        F.trim("k").alias("clean_k"),
        F.length("k").alias("k_length"),
    )
)

result = (
    filtered
    .select(
        "id",
        "k",
        "value",
        "k_title",
        "k_length",
        F.when(F.col("k").rlike("^(trace|region)$"), "CORE").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("k_length").desc())
)
