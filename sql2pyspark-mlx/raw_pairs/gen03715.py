from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")
w4 = Window.partitionBy(F.year("last_restock_plus_months")).orderBy(F.col("quantity").asc_nulls_last())
w_ordered = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("quantity").between(102, 174))
    .select("*", F.add_months("last_restock", 6).alias("last_restock_plus_months"))
)

enriched = (
    base
    .select(
        "*",
        F.max("quantity").over(w).alias("product_id_max_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_3_prior"),
        F.ntile(5).over(w3).alias("quantity_bucket"),
        F.percent_rank().over(w4).alias("quantity_percent_rank"),
    )
)

enriched_inventory = (
    enriched
    .select(
        "id",
        "last_restock_plus_months",
        "quantity_bucket",
        F.sum("quantity_avg_3_prior").over(w_ordered).alias("last4_sum_quantity_avg_3_prior"),
        F.count("quantity_avg_3_prior").over(w_ordered).alias("last4_count_quantity_avg_3_prior"),
        F.avg("quantity_avg_3_prior").over(w_ordered).alias("last4_avg_quantity_avg_3_prior"),
        F.min("quantity_avg_3_prior").over(w_ordered).alias("last4_min_quantity_avg_3_prior"),
    )
)

cleaned = (
    enriched_inventory
    .filter(F.col("quantity_bucket") >= 6)
    .withColumn("last4_min_quantity_avg_3_prior_rounded", F.round("last4_min_quantity_avg_3_prior", 2))
)

totals = (
    cleaned
    .withColumn("last4_avg_quantity_avg_3_prior_tier", F.when(F.col("last4_avg_quantity_avg_3_prior") >= 208, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock_plus_months"))
    .withColumn("last4_min_quantity_avg_3_prior_rounded_tier", F.when(F.col("last4_min_quantity_avg_3_prior_rounded") >= 89, "HIGH").otherwise("LOW"))
    .cube(
        "last4_avg_quantity_avg_3_prior_tier",
        "quarter_start",
        "last4_min_quantity_avg_3_prior_rounded_tier",
    )
    .agg(
        F.percentile_approx("last4_count_quantity_avg_3_prior", [0.25, 0.5, 0.75]).alias("last4_count_quantity_avg_3_prior_quartiles"),
        F.percentile_approx("last4_min_quantity_avg_3_prior_rounded", [0.25, 0.5, 0.75]).alias("last4_min_quantity_avg_3_prior_rounded_quartiles"),
        F.percentile_approx("last4_avg_quantity_avg_3_prior", [0.25, 0.5, 0.75]).alias("last4_avg_quantity_avg_3_prior_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    totals
    .select(
        "last4_avg_quantity_avg_3_prior_tier",
        "last4_count_quantity_avg_3_prior_quartiles",
        "last4_min_quantity_avg_3_prior_rounded_quartiles",
        "last4_avg_quantity_avg_3_prior_quartiles",
        "distinct_ids",
        "event_count",
        "grouping_level",
    )
    .orderBy(F.col("grouping_level").asc_nulls_last())
)
