from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy(F.year("last_restock"), "store_id")
w6 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_rounded").asc_nulls_last())
w7 = Window.orderBy("last_restock_year_sum_quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("running_min_quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-09-08").cast("date"))
    .select("*", F.round("quantity", 1).alias("quantity_rounded"))
)

base_inventory = (
    base
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-05-18"))) & F.col("product_id").isin([2, 5, 7, 10]))
    .select(
        "*",
        F.date_trunc("month", "last_restock").alias("last_restock_period"),
        F.when(F.col("quantity") >= 246, "High").when(F.col("quantity") >= 95, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

ranked = (
    base_inventory
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    ranked
    .select(
        "id",
        "quantity",
        "quantity_rounded",
        "last_restock_period",
        "quantity_level",
        F.lead("quantity_rounded", 1).over(w2).alias("next_quantity_rounded"),
        F.sum("quantity_rounded").over(w3).alias("product_id_sum_quantity_rounded"),
        F.count("quantity").over(w4).alias("quantity_count_30d"),
        F.sum("quantity_rounded").over(w5).alias("last_restock_year_sum_quantity_rounded"),
        F.dense_rank().over(w6).alias("quantity_rounded_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "quantity_rounded",
        "last_restock_period",
        "next_quantity_rounded",
        "product_id_sum_quantity_rounded",
        "last_restock_year_sum_quantity_rounded",
        F.avg("quantity_rounded").over(w7).alias("running_avg_quantity_rounded"),
        F.min("quantity_rounded").over(w7).alias("running_min_quantity_rounded"),
        F.count("quantity_rounded").over(w7).alias("running_count_quantity_rounded"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("running_min_quantity_rounded").over(w_hist).alias("running_running_min_quantity_rounded"),
        F.sum("last_restock_year_sum_quantity_rounded").over(w7).alias("cumulative_last_restock_year_sum_quantity_rounded"),
    )
)

enriched_inventory = (
    enriched
    .filter(F.col("running_running_min_quantity_rounded") != 251)
    .withColumn("running_min_running_running_min_quantity_rounded", F.min("running_running_min_quantity_rounded").over(w_hist))
    .withColumn("running_count_running_running_min_quantity_rounded", F.count("running_running_min_quantity_rounded").over(w_hist))
    .withColumn("running_sum_running_running_min_quantity_rounded", F.sum("running_running_min_quantity_rounded").over(w_hist))
)

result = (
    enriched_inventory
    .withColumn("year_start", F.date_trunc("YEAR", "last_restock_period"))
    .rollup("year_start")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("next_quantity_rounded", [0.25, 0.5, 0.75]).alias("next_quantity_rounded_quartiles"),
        F.max("last_restock_period").alias("last_seen"),
        F.min("quantity_rounded").alias("min_quantity_rounded"),
        F.avg("cumulative_last_restock_year_sum_quantity_rounded").alias("avg_cumulative_last_restock_year_sum_quantity_rounded"),
        F.sum("quantity_rounded").alias("total_quantity_rounded"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("min_quantity_rounded").desc())
)
