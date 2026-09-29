from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 61, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.avg(F.when(F.col("plan") == "basic", F.col("element"))).alias("basic_avg_element"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("basic_avg_element").asc_nulls_last())
)
