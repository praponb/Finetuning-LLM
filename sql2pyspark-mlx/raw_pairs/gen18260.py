from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("element")

lines = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("element"))

result = (
    lines
    .select(
        "id",
        "currency",
        "element",
        F.dense_rank().over(w).alias("element_dense_rank"),
    )
)
