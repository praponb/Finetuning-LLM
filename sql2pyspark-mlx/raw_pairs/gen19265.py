from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("coupon_codes_count").desc_nulls_last())
w3 = Window.partitionBy("k")
w4 = Window.partitionBy(F.month("created_at"), "k")
w_grp = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("k").orderBy(F.col("coupon_codes_count_percent_rank").desc_nulls_first())
w7 = Window.partitionBy(F.year("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.hour("created_at").between(11, 18))
    .withColumn("k_filled", F.coalesce("k", F.lit("Unknown")))
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
    .withColumn("k_clean", F.coalesce("k", F.lit("OTHER")))
)

scored = (
    valid
    .select(
        "*",
        F.avg("coupon_codes_count").over(w_recent).alias("coupon_codes_count_avg_2h"),
        F.percent_rank().over(w2).alias("coupon_codes_count_percent_rank"),
    )
)

metrics = (
    scored
    .filter(F.col("coupon_codes_count") < 62)
    .withColumn("k_sum_coupon_codes_count_avg_2h", F.sum("coupon_codes_count_avg_2h").over(w3))
    .withColumn("created_at_month_max_coupon_codes_count", F.max("coupon_codes_count").over(w4))
    .withColumn("coupon_codes_count_percent_rank_max_15m", F.max("coupon_codes_count_percent_rank").over(w_grp))
    .withColumn("coupon_codes_count_percent_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("coupon_codes_count_avg_2h_2", F.avg("coupon_codes_count").over(w7))
)

result = (
    metrics
    .select(
        "id",
        "k",
        "k_filled",
        "coupon_codes_count",
        "k_clean",
        "coupon_codes_count_percent_rank",
        "k_sum_coupon_codes_count_avg_2h",
        "created_at_month_max_coupon_codes_count",
        F.when(F.col("k").rlike("^(weight_kg|device)$"), "EXTENDED").otherwise("CORE").alias("k_zone"),
    )
    .orderBy("created_at_month_max_coupon_codes_count", F.col("id").desc())
)
