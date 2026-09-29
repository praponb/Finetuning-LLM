from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w_part = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w_running = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy("city").orderBy("rating_dense_rank", "id")
w5 = Window.orderBy("seq_no", "id")
w6 = Window.partitionBy("city").orderBy(F.col("seq_no").desc(), "id")
w_grp = Window.partitionBy("city").orderBy(F.col("next_rating_dense_rank").asc_nulls_first())
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("city").orderBy(F.col("prev_seq_no").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select("*", F.trim("country").alias("clean_country"))
)

scored = (
    valid
    .filter(F.col("rating") < 4.0)
    .select(
        "id",
        "city",
        "clean_country",
        F.percent_rank().over(w).alias("rating_percent_rank"),
        F.dense_rank().over(w_part).alias("rating_dense_rank"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "city",
        "rating_dense_rank",
        "seq_no",
        F.lag("seq_no").over(w4).alias("prev_seq_no"),
        F.lead("seq_no").over(w4).alias("next_seq_no"),
        F.lag("rating_dense_rank").over(w4).alias("prev_rating_dense_rank"),
        F.lead("rating_dense_rank").over(w4).alias("next_rating_dense_rank"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.lead("seq_no", 1).over(w5).alias("next_seq_no_2"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "prev_seq_no", "next_rating_dense_rank")
)

enriched_suppliers = (
    ranked
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("next_rating_dense_rank_cume_dist"),
        F.avg("next_rating_dense_rank").over(w8).alias("city_avg_next_rating_dense_rank"),
        F.max("next_rating_dense_rank").over(w8).alias("city_max_next_rating_dense_rank"),
    )
)

latest = (
    enriched_suppliers
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "prev_seq_no",
        "next_rating_dense_rank",
        "next_rating_dense_rank_cume_dist",
        "city_avg_next_rating_dense_rank",
        "city_max_next_rating_dense_rank",
    )
)
