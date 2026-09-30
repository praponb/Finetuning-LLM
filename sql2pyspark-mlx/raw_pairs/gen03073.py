from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("element")

lines = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("element"))

result = (
    lines
    .select(
        "id",
        "plan",
        "element",
        F.max("element").over(w).alias("plan_max_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
    )
)
