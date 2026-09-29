from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("element")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    lines
    .select(
        "effective_from",
        "element",
        F.cume_dist().over(w).alias("element_cume_dist"),
    )
)
