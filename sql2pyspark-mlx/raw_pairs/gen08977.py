from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w_grp = Window.partitionBy("city").orderBy("dedup_rank", "id")

valid = (
    df_suppliers
    .filter(F.col("country") == "SG")
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .filter(F.col("rating") >= 2.3)
    .select(
        "id",
        "rating",
        "supplier_name_title",
        "resolved_city",
        "dedup_rank",
        F.lag("rating").over(w_grp).alias("prev_rating"),
        F.lead("rating").over(w_grp).alias("next_rating"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").isNotNull()), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("next_rating") >= 4.0, "HIGH").otherwise("LOW").alias("next_rating_tier"),
    )
    .agg(
        F.percentile_approx("next_rating", [0.25, 0.5, 0.75]).alias("next_rating_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("prev_rating").alias("max_prev_rating"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.avg("next_rating").alias("avg_next_rating"),
    )
    .orderBy(F.col("distinct_ids_3").asc_nulls_first())
)
