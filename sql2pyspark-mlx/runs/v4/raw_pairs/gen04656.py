from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.orderBy(F.col("entry_price").desc_nulls_last())
w_ordered_2 = Window.orderBy("entry_price", "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.avg("entry_price").over(w).alias("entry_price_avg_5"),
        F.percent_rank().over(w_ordered).alias("entry_price_percent_rank"),
        F.row_number().over(w_ordered_2).alias("seq_no"),
        F.ntile(4).over(w_ordered_2).alias("entry_price_bucket"),
        F.row_number().over(w_ordered_2).alias("event_seq"),
    )
)
