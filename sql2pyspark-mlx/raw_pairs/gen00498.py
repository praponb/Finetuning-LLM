from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc())
w2 = Window.orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        "currency",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.dense_rank().over(w3).alias("val_dense_rank_2"),
    )
)
