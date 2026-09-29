from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "plan",
        F.count("entry_price").over(w).alias("running_count_entry_price"),
        F.avg("entry_price").over(w).alias("running_avg_entry_price"),
        F.max("entry_price").over(w).alias("running_max_entry_price"),
        F.sum("entry_price").over(w).alias("running_sum_entry_price"),
    )
)
