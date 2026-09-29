from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct_scaled").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("promo_code").orderBy(F.col("event_seq").desc_nulls_last(), "id")

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-06-14").cast("date")) & F.col("channel").isNotNull())
    .withColumn("discount_pct_scaled", F.round((F.col("discount_pct") * 0.9), 2))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

with_windows = (
    base
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w3))
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

daily = (
    ranked
    .groupBy(
        F.when(F.col("discount_pct_avg_5_prior") >= 0, "HIGH").otherwise("LOW").alias("discount_pct_avg_5_prior_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount_pct")).otherwise(0)).alias("web_discount_pct"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("row_seq_per_spring10"),
        F.round(F.sum("discount_pct_scaled") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_scaled_per_partner"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
    )
)

result = (
    daily
    .select("discount_pct_avg_5_prior_tier", "cnt", "web_discount_pct", "web_rows")
    .orderBy("discount_pct_avg_5_prior_tier")
    .limit(5)
)
