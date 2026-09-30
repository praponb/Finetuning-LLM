from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w_recent = Window.partitionBy("city", "country").orderBy("rank_in_group", "id")
w_hist = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("last_known_rating", "id")
w5 = Window.partitionBy("country", "city").orderBy("event_seq", "id")
w6 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_suppliers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.lag("rating").over(w_recent).alias("prev_rating"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.last("rating", ignorenulls=True).over(w_hist).alias("last_known_rating"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) >= 2), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    ranked
    .filter((F.col("last_known_rating") >= 3.4) & F.col("city").isNotNull())
    .withColumn("clean_city", F.trim("city"))
    .withColumn("city_filled", F.coalesce("city", F.lit("NONE")))
)

scored = (
    cleaned
    .filter(F.col("event_seq") >= 4)
    .withColumn("prev_last_known_rating", F.lag("last_known_rating").over(w5))
    .withColumn("next_prev_rating", F.lead("prev_rating").over(w5))
    .withColumn("event_seq_2", F.row_number().over(w5))
    .withColumn("running_rating", F.sum("rating").over(w6))
)

result = (
    scored
    .select(
        "id",
        "supplier_name",
        "rating",
        "rank_in_group",
        "prev_rating",
        "event_seq",
        "last_known_rating",
        "dedup_rank",
        "city_filled",
        F.when(F.col("supplier_name").rlike("^(Tokyo Traders|Global Supply)$"), "GROUP_B").when(F.col("supplier_name").rlike("^(Siam Parts Co|EuroGoods GmbH)$"), "EXTENDED").otherwise("CORE").alias("supplier_name_zone"),
    )
    .orderBy(F.col("dedup_rank").desc(), F.col("rating").desc())
)
