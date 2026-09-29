from pyspark.sql import functions as F

with_dims = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.amount"))
)

result = (
    with_dims
    .withColumn("year_start", F.date_trunc("YEAR", "shipped_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .groupingSets(
        [["year_start", "quarter_start"], ["year_start"]],
        "year_start",
        "quarter_start",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.avg(F.when(F.col("status") == "LOST", F.col("cost"))).alias("lost_avg_cost"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("num_status").desc_nulls_first())
)
