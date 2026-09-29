from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("lowest_price", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("lowest_price", "id")
w_grp = Window.orderBy(F.col("effective_from").desc(), "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.avg("lowest_price").over(w).alias("lowest_price_avg_6_prior"),
        F.ntile(4).over(w2).alias("lowest_price_bucket"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)
