from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("product_id").asc_nulls_first())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select("id", "created_at", F.col("elem.product_id"))
)

scored = (
    lines
    .withColumn("product_id_rank", F.rank().over(w_grp))
    .withColumn("product_id_avg_1h", F.avg("product_id").over(w2))
)

cleaned = (
    scored
    .filter(F.col("product_id").between(6, 9))
    .withColumn("product_id_rank_band", F.when(F.col("product_id_rank") >= 9, "High").when(F.col("product_id_rank") >= 4, "Medium").otherwise("Low"))
)

filtered = (
    cleaned
    .filter(F.col("created_at") >= F.lit("2025-03-01 20:00:00").cast("timestamp"))
    .select(
        "id",
        "created_at",
        "product_id_rank",
        "product_id_avg_1h",
        "product_id_rank_band",
        F.round("product_id", 2).alias("product_id_rounded"),
        F.greatest("product_id", "product_id_avg_1h").alias("greatest_product_id_product_id_avg_1h"),
    )
)

result = (
    filtered
    .select(
        "created_at",
        "product_id_rank",
        "product_id_avg_1h",
        "product_id_rank_band",
        "product_id_rounded",
        "greatest_product_id_product_id_avg_1h",
    )
    .orderBy(
        "greatest_product_id_product_id_avg_1h",
        F.col("product_id_rounded").desc(),
    )
)
