from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("val")
w_recent = Window.orderBy(F.col("val").desc())
w_recent_2 = Window.orderBy(F.col("val").asc_nulls_first())
w4 = Window.orderBy(F.col("val").desc_nulls_last())

flattened = df_price_tiers.select("id", "effective_from", F.explode("tier_limits").alias("val"))

kept_keys = (
    flattened
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan").isin(["basic", "pro", "enterprise"]))
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("effective_from") >= F.lit("2024-06-02").cast("date"))
    .withColumn("effective_from_plus_65d", F.date_add("effective_from", 65))
)

enriched = (
    base
    .select(
        "id",
        "effective_from",
        F.dense_rank().over(w_part).alias("val_dense_rank"),
        F.cume_dist().over(w_recent).alias("val_cume_dist"),
        F.rank().over(w_recent_2).alias("val_rank"),
        F.dense_rank().over(w_recent).alias("val_dense_rank_2"),
        F.dense_rank().over(w4).alias("val_dense_rank_3"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("val_dense_rank_2") >= 7, "LARGE").otherwise("SMALL").alias("val_dense_rank_2_tier"),
        F.date_trunc("year", "effective_from").alias("year_start"),
    )
    .agg(
        F.sum("val_rank").alias("total_val_rank"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.min("val_rank").alias("min_val_rank"),
    )
    .filter(F.col("cnt") > 1)
)
