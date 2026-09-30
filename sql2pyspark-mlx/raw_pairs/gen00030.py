from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("browser") == "Edge")
        .select("country")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("customer_id", "browser")
    .agg(
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.count("*").alias("num_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
