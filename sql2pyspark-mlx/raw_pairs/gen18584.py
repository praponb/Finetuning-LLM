from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("effective_from"), "currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "effective_from",
        "tier_value",
        F.avg("tier_value").over(w).alias("effective_from_month_avg_tier_value"),
    )
)
