from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

result = flattened.select("plan", "effective_from", F.rank().over(w).alias("val_rank"))
