from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_first(), "id")
w2 = Window.orderBy("latest_rank_per_rating", "id")
w3 = Window.partitionBy("city").orderBy(F.col("latest_rank").desc(), "id")
w_part = Window.partitionBy("country", "city").orderBy("latest_rank_per_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("latest_rank", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("latest_rank_per_rating").desc_nulls_last(), "id")

latest = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("latest_rank") < 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "latest_rank",
        F.round((F.col("latest_rank") / F.col("rating")), 2).alias("latest_rank_per_rating"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lead("latest_rank_per_rating", 1).over(w2).alias("next_latest_rank_per_rating"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("latest_rank_per_rating").over(w_part).alias("cumulative_latest_rank_per_rating"),
        F.avg("latest_rank_per_rating").over(w5).alias("latest_rank_per_rating_avg_3"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "supplier_name",
        "city",
        "latest_rank",
        "latest_rank_per_rating",
        "next_latest_rank_per_rating",
        "seq_no",
        "cumulative_latest_rank_per_rating",
        "latest_rank_per_rating_avg_3",
        F.when(F.col("supplier_name").rlike("^(Siam Parts Co|Pacific Wholesale)$"), "CORE").otherwise("EXTENDED").alias("supplier_name_zone"),
    )
    .orderBy(F.col("cumulative_latest_rank_per_rating").desc())
)
