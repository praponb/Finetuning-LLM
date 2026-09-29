from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("browser")
    .intersectAll(
        df_sessions
        .filter(F.col("device").isin(["mobile", "tablet"]))
        .select("browser")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("customer_id", "device", "browser")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .filter(F.col("distinct_customer_ids") >= 1)
    .orderBy(F.col("browser").desc())
)
