from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

result = (
    lines
    .filter(F.col("currency") == "EUR")
    .select("plan", "currency", F.rank().over(w).alias("val_rank"))
)
