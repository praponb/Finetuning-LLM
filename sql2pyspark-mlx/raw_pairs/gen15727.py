from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.avg("discount_pct").over(w).alias("last7_avg_discount_pct"),
        F.sum("discount_pct").over(w).alias("last7_sum_discount_pct"),
        F.count("discount_pct").over(w).alias("last7_count_discount_pct"),
        F.min("discount_pct").over(w).alias("last7_min_discount_pct"),
    )
)

result = (
    scored
    .select(
        "id",
        "channel",
        "discount_pct",
        "last7_count_discount_pct",
        "last7_min_discount_pct",
    )
    .orderBy(F.col("id").desc_nulls_last(), "channel")
)
