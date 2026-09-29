from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.year("starts_on"), "channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w3 = Window.partitionBy("promo_code").orderBy("prev_discount_pct", "id")
w4 = Window.orderBy("is_spring10", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_promotions
    .filter(F.col("promo_code").isin(["VIP", "SPRING10"]))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.lag("discount_pct", 2, 0).over(w_grp).alias("prev_discount_pct"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

base = (
    scored
    .filter(F.col("ends_on") > F.lit("2023-10-25").cast("date"))
    .withColumn("promo_code_title", F.initcap(F.trim("promo_code")))
    .withColumn("is_spring10", F.when(F.col("promo_code") == "SPRING10", 1).otherwise(0))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "promo_code", "prev_discount_pct", "is_spring10")
)

cleaned = (
    latest
    .filter(F.col("is_spring10") != 39)
    .select("*", F.length("promo_code").alias("promo_code_length"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.lead("prev_discount_pct").over(w3).alias("next_prev_discount_pct"),
        F.lag("is_spring10").over(w3).alias("prev_is_spring10"),
        F.lead("is_spring10").over(w3).alias("next_is_spring10"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("prev_discount_pct", ignorenulls=True).over(w4).alias("last_known_prev_discount_pct"),
    )
)

result = (
    with_windows
    .withColumn("next_is_spring10_tier", F.when(F.col("next_is_spring10") >= 86, "PREMIUM").otherwise("STANDARD"))
    .cube("next_is_spring10_tier")
    .agg(
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("prev_discount_pct"))).alias("spring10_total_prev_discount_pct"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("spring10_total_prev_discount_pct")
)
