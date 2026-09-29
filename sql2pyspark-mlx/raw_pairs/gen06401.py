from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.partitionBy("currency").orderBy("element")

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

result = (
    exploded
    .select(
        "id",
        "currency",
        "element",
        F.max("element").over(w).alias("currency_max_element"),
        F.rank().over(w2).alias("element_rank"),
        F.rank().over(w3).alias("element_rank_2"),
    )
)
