from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

result = (
    exploded
    .filter(F.col("element") >= 77)
    .select(
        "plan",
        "currency",
        F.stddev_samp("element").over(w_part).alias("plan_stddev_samp_element"),
        F.sum("element").over(w_part).alias("plan_sum_element"),
    )
)
