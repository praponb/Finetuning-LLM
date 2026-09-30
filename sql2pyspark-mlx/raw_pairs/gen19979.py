from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

result = (
    exploded
    .filter(F.col("effective_from") > "2024-05-01")
    .select(
        "plan",
        "effective_from",
        "currency",
        F.max("element").over(w).alias("plan_max_element"),
    )
)
