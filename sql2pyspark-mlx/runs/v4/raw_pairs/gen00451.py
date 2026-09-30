from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan")
w_grp_2 = Window.orderBy(F.col("element").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .select(
        "*",
        F.avg("element").over(w_grp).alias("plan_avg_element"),
        F.percent_rank().over(w_grp_2).alias("element_percent_rank"),
    )
)
