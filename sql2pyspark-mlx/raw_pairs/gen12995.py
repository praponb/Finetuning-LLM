from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("tier_value").desc())

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    lines
    .select(
        "id",
        "plan",
        "tier_value",
        F.percent_rank().over(w_running).alias("tier_value_percent_rank"),
    )
)
