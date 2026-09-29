from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    exploded
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .groupingSets(
        [["quarter_start", "activity_month", "plan"], ["quarter_start", "activity_month"], []],
        "quarter_start",
        "activity_month",
        "plan",
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
    .orderBy(F.col("basic_count").desc())
)
