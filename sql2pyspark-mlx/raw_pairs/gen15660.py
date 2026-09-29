from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "plan",
        F.count("tier_value").over(w_grp).alias("plan_count_tier_value"),
        F.stddev_samp("tier_value").over(w_grp).alias("plan_stddev_samp_tier_value"),
        F.avg("tier_value").over(w_grp).alias("plan_avg_tier_value"),
    )
)
