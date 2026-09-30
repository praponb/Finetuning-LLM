from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id", "city").orderBy("running_avg_cost", "id")

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-07-02").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("tags_text", F.array_join("tags", ", "))
)

enriched = (
    valid
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.cost",
        "e.supplier_id",
        "e.tags_text",
        F.col("b.country"),
        F.col("b.city"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id") == 9), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "cost",
        "supplier_id",
        "country",
        "city",
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "cost", "supplier_id", "city", "running_count_cost")
)

totals = (
    latest
    .groupBy(
        F.when(F.col("running_count_cost") >= 14, "LARGE").otherwise("SMALL").alias("running_count_cost_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.round(F.sum("running_count_cost") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("running_count_cost_per_singapore"),
        F.sum("running_count_cost").alias("total_running_count_cost"),
        F.percentile_approx("running_count_cost", [0.25, 0.5, 0.75]).alias("running_count_cost_quartiles"),
    )
)

result = (
    totals
    .select(
        "running_count_cost_tier",
        "running_count_cost_per_singapore",
        "total_running_count_cost",
        "running_count_cost_quartiles",
    )
)
