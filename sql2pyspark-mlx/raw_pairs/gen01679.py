from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("brand").orderBy(F.col("price").asc_nulls_last())
w3 = Window.partitionBy("brand").orderBy(F.col("price_dense_rank").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("brand").orderBy(F.col("prev_cost").asc_nulls_last(), "id")
w6 = Window.partitionBy("brand").orderBy(F.col("seq_no").desc(), "id")

base = (
    df_products
    .filter(F.col("product_name").isNotNull() & (F.col("launch_date") > F.lit("2023-07-24").cast("date")))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        F.array_join("tags", ", ").alias("tags_text"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

metrics = (
    base
    .select(
        "id",
        "brand",
        "tags_text",
        "brand_clean",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lag("price").over(w).alias("prev_price"),
        F.lead("price").over(w).alias("next_price"),
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.units") != 23), "left_semi")
)

ranked = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

deduped = (
    with_last
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("prev_cost", 0.95).alias("prev_cost_p95"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.round(F.avg("prev_price"), 2).alias("avg_prev_price"),
    )
)
