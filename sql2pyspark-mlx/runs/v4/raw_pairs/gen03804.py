from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.avg(F.when(F.col("currency") == "USD", F.col("element"))).alias("usd_avg_element"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("usd_avg_element")
)
