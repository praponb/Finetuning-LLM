from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        "currency",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("starter_rows").desc())
)
