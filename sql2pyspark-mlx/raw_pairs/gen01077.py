from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .select(
        "*",
        F.rank().over(w_grp).alias("tier_value_rank"),
        F.percent_rank().over(w_grp).alias("tier_value_percent_rank"),
    )
)
