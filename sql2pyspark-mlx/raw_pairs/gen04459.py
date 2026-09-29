from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w_recent = Window.partitionBy("plan")

flattened = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

result = (
    flattened
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.min("element").over(w_recent).alias("plan_min_element"),
        F.max("element").over(w_recent).alias("plan_max_element"),
    )
)
