from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc())
w3 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("city", "country").orderBy("rating_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("rating").desc(), "id")
w6 = Window.partitionBy("country", "city").orderBy(F.col("rating_dense_rank").asc_nulls_last(), "id")
w7 = Window.partitionBy("city", "country").orderBy("rating_rank", "id")
w8 = Window.partitionBy("country")
w9 = Window.orderBy("seq_no", "id")
w_part = Window.partitionBy("city").orderBy("seq_no", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("rating") >= 1.9)
    .withColumn("rating_whole", F.floor("rating"))
)

enriched = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("rating_rank"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
        F.avg("rating").over(w3).alias("rating_avg_3"),
        F.sum("rating_whole").over(w4).alias("cumulative_rating_whole"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

cleaned = (
    enriched
    .filter(F.col("rating_avg_3") > 3)
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_whole",
        "rating_rank",
        "rating_dense_rank",
        "rating_avg_3",
        "seq_no",
        (F.col("rating_rank") / F.nullif("rating_whole", F.lit(0))).alias("rating_rank_per_rating_whole"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .withColumn("next_rating_avg_3", F.lead("rating_avg_3", 1).over(w7))
    .withColumn("rating_rank_share", F.round((F.col("rating_rank") / F.sum("rating_rank").over(w8)), 4))
    .withColumn("next_rating_whole", F.lead("rating_whole", 1).over(w9))
    .withColumn("seq_no_avg_3", F.avg("seq_no").over(w_part))
)
