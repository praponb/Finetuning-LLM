from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("region").orderBy("running_count_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy(F.col("running_min_sqft").desc_nulls_last())

base = (
    df_stores
    .filter(F.month("opened_date").isin([4, 8, 10]))
    .select(
        "*",
        F.regexp_replace("store_name", " ", "-").alias("store_name_slug"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

enriched = (
    base
    .filter(F.col("opened_date") > "2024-05-21")
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "region_filled",
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.count("sqft").over(w).alias("running_count_sqft"),
        F.max("sqft").over(w).alias("running_max_sqft"),
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

scored = (
    latest_match
    .select(
        "id",
        "opened_date",
        "sqft",
        "region_filled",
        "running_sum_sqft",
        "running_count_sqft",
        "sqft_cume_dist",
        F.sum("sqft_cume_dist").over(w4).alias("running_sum_sqft_cume_dist"),
        F.min("sqft_cume_dist").over(w4).alias("running_min_sqft_cume_dist"),
        F.count("sqft_cume_dist").over(w4).alias("running_count_sqft_cume_dist"),
        F.avg("sqft_cume_dist").over(w4).alias("running_avg_sqft_cume_dist"),
        F.rank().over(w5).alias("running_min_sqft_rank"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_anti")
)

report = (
    not_flagged
    .select(
        "id",
        "opened_date",
        "sqft",
        "region_filled",
        "running_count_sqft",
        "running_sum_sqft_cume_dist",
        "running_min_sqft_cume_dist",
    )
    .orderBy(F.col("id").desc())
)

result = (
    report
    .select(
        "id",
        "opened_date",
        "sqft",
        "region_filled",
        "running_count_sqft",
        "running_sum_sqft_cume_dist",
        "running_min_sqft_cume_dist",
    )
    .orderBy(
        F.col("running_count_sqft").desc_nulls_first(),
        F.col("running_min_sqft_cume_dist").desc(),
    )
)
