from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("entry_price").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.ntile(4).over(w).alias("entry_price_bucket"),
        F.avg("entry_price").over(w2).alias("entry_price_avg_2_prior"),
        F.sum("entry_price").over(w3).alias("running_entry_price"),
        F.ntile(4).over(w).alias("entry_price_bucket_2"),
    )
)
