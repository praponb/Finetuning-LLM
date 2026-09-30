from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code")

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-05-09").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.dayofweek("ends_on").alias("ends_on_dayofweek"),
    )
)

cleaned = (
    valid
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-08-20"))) & F.col("channel").isNotNull())
    .withColumn("promo_code_length", F.length("promo_code"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "ends_on_dayofweek",
        F.min("discount_pct_whole").over(w).alias("running_min_discount_pct_whole"),
        F.count("discount_pct_whole").over(w).alias("running_count_discount_pct_whole"),
        F.sum("discount_pct_whole").over(w).alias("running_sum_discount_pct_whole"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "starts_on",
        "ends_on_dayofweek",
        F.first("ends_on_dayofweek", ignorenulls=True).over(w).alias("first_known_ends_on_dayofweek"),
        F.avg("running_min_discount_pct_whole").over(w2).alias("promo_code_avg_running_min_discount_pct_whole"),
    )
)

cleaned_promotions = (
    enriched
    .filter(F.col("promo_code_avg_running_min_discount_pct_whole") <= 69)
    .withColumn("first_known_ends_on_dayofweek_per_promo_code_avg_running_min_discount_pct_whole", (F.col("first_known_ends_on_dayofweek") / F.nullif("promo_code_avg_running_min_discount_pct_whole", F.lit(0))))
    .withColumn("first_known_ends_on_dayofweek_minus_ends_on_dayofweek", (F.col("first_known_ends_on_dayofweek") - F.col("ends_on_dayofweek")))
)

result = (
    cleaned_promotions
    .select(
        "id",
        "first_known_ends_on_dayofweek_minus_ends_on_dayofweek",
        "first_known_ends_on_dayofweek_per_promo_code_avg_running_min_discount_pct_whole",
    )
    .unpivot(
        ids=["id"],
        values=["first_known_ends_on_dayofweek_minus_ends_on_dayofweek", "first_known_ends_on_dayofweek_per_promo_code_avg_running_min_discount_pct_whole"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
