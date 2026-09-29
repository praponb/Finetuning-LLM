from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

lines = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("element"))

result = (
    lines
    .select("id", "plan", "element", F.avg("element").over(w).alias("plan_avg_element"))
)
