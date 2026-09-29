from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w3 = Window.partitionBy("store_id").orderBy("dedup_rank", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-05-04")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        (F.datediff(F.current_date(), "last_restock") / 365.25).alias("years_since_last_restock"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "quantity", "last_restock", "last_restock_label")
)

deduped_inventory = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

deduped_inventory_2 = (
    deduped_inventory
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    deduped_inventory_2
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "last_restock_label",
        "dedup_rank",
        F.greatest("dedup_rank", "quantity").alias("greatest_dedup_rank_quantity"),
    )
)

summary = (
    valid
    .groupBy(F.date_trunc("year", "last_restock").alias("year_start"))
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("greatest_dedup_rank_quantity"), 2).alias("avg_greatest_dedup_rank_quantity"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .select(
        "year_start",
        "quantity_quartiles",
        "avg_greatest_dedup_rank_quantity",
        "cnt",
    )
)
