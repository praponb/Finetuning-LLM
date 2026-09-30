from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc())
w2 = Window.orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("val"))
)

with_windows = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_dense_rank", F.dense_rank().over(w3))
)

cleaned = (
    with_windows
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .withColumn("val_rank_whole", F.floor("val_rank"))
)

result = (
    cleaned
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "val_rank",
        "val_dense_rank",
        "val_rank_whole",
        F.when(F.col("plan").rlike("^(starter|enterprise)$"), "PRIORITY").when(F.col("plan").rlike("^(basic|pro)$"), "EXTENDED").otherwise("CORE").alias("plan_zone"),
    )
)
