from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("plan_length").asc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("currency").orderBy(F.col("plan_length_filled").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("starter_rows").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("element"))
)

filtered = (
    flattened
    .filter(F.col("element") >= 25)
    .withColumn("plan_title", F.initcap(F.trim("plan")))
    .withColumn("currency_prefix", F.substring("currency", 1, 3))
)

cleaned = (
    filtered
    .filter(F.col("plan") != "enterprise")
    .withColumn("plan_length", F.length("plan"))
)

filtered_price_tiers = (
    cleaned
    .filter(F.col("element") < 34)
    .withColumn("plan_length_filled", F.coalesce("plan_length", F.lit(0)))
    .withColumn("clean_plan", F.trim("plan"))
)

metrics = (
    filtered_price_tiers
    .withColumn("plan_length_percent_rank", F.percent_rank().over(w))
    .withColumn("plan_max_plan_length", F.max("plan_length").over(w2))
    .withColumn("plan_length_filled_dense_rank", F.dense_rank().over(w3))
    .withColumn("plan_sum_element", F.sum("element").over(w2))
)

result = (
    metrics
    .groupBy(
        "currency",
        F.when(F.col("plan_length_percent_rank") >= 0.8, "PREMIUM").otherwise("STANDARD").alias("plan_length_percent_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.percentile_approx("plan_max_plan_length", [0.25, 0.5, 0.75]).alias("plan_max_plan_length_quartiles"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.round(F.sum("plan_max_plan_length") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("plan_max_plan_length_per_usd"),
        F.percentile_approx("plan_length", [0.25, 0.5, 0.75]).alias("plan_length_quartiles"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .withColumn("starter_rows_rank", F.rank().over(w4))
    .orderBy("currency")
)
