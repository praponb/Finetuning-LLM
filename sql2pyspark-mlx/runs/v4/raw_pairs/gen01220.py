from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .groupingSets(
        [["quarter_start", "store_id", "week_start"], ["quarter_start", "store_id"], ["quarter_start"]],
        "quarter_start",
        "store_id",
        "week_start",
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("amount_per_partner"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("quarter_start").alias("gid"),
    )
)
