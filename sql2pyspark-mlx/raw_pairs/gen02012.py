from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.orderBy("discount_pct", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("last7_sum_discount_pct")
w4 = Window.orderBy(F.col("last7_count_discount_pct_num_minus_last7_max_discount_pct").desc_nulls_last())
w5 = Window.orderBy(F.col("last7_count_discount_pct_num").desc_nulls_last())
w6 = Window.orderBy("discount_pct")

latest = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "discount_pct")
)

enriched = (
    latest
    .filter(F.col("discount_pct") < 0.31)
    .select(
        "*",
        F.count("discount_pct").over(w2).alias("last7_count_discount_pct"),
        F.sum("discount_pct").over(w2).alias("last7_sum_discount_pct"),
        F.max("discount_pct").over(w2).alias("last7_max_discount_pct"),
    )
)

base = (
    enriched
    .filter(F.col("last7_sum_discount_pct") < 1.58)
    .withColumn("last7_count_discount_pct_num", F.col("last7_count_discount_pct").cast("double"))
)

matched_keys = (
    base
    .select("id")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct") >= 0.12)
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("last7_count_discount_pct").between(8, 10))
    .withColumn("last7_count_discount_pct_num_minus_last7_max_discount_pct", (F.col("last7_count_discount_pct_num") - F.col("last7_max_discount_pct")))
)

result = (
    valid
    .select(
        "id",
        "discount_pct",
        "last7_count_discount_pct",
        "last7_count_discount_pct_num",
        "last7_count_discount_pct_num_minus_last7_max_discount_pct",
        F.percent_rank().over(w3).alias("last7_sum_discount_pct_percent_rank"),
        F.percent_rank().over(w4).alias("last7_count_discount_pct_num_minus_last7_max_discount_pct_percent_rank"),
        F.cume_dist().over(w5).alias("last7_count_discount_pct_num_cume_dist"),
        F.cume_dist().over(w5).alias("last7_count_discount_pct_num_cume_dist_2"),
        F.cume_dist().over(w6).alias("discount_pct_cume_dist"),
    )
)
