from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

result = (
    exploded
    .filter(F.col("val") <= 72)
    .select("id", "plan", F.sum("val").over(w).alias("plan_sum_val"))
)
