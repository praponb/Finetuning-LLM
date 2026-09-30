from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("element"))
)

result = (
    exploded
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.sum("element").alias("total_element"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("element", 0.5).alias("element_p50"),
        F.count("*").alias("cnt"),
        F.round(F.avg("element"), 2).alias("avg_element"),
    )
)
