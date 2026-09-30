from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("element").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .filter(F.col("element") < 41)
    .select("id", "plan", "element", F.cume_dist().over(w).alias("element_cume_dist"))
)
