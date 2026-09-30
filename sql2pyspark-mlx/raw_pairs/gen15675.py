from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_last())
w_grp = Window.partitionBy("product_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-04-04").cast("date"))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

joined = (
    base
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        "a.quantity_filled",
        F.col("b.product_name"),
    )
)

ranked = (
    joined
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .filter(F.col("rank_in_group") != 1)
    .select(
        "id",
        "product_id",
        "quantity",
        "product_name",
        "rank_in_group",
        F.min("quantity_filled").over(w_grp).alias("running_min_quantity_filled"),
        F.sum("quantity_filled").over(w_grp).alias("running_sum_quantity_filled"),
        F.max("quantity_filled").over(w_grp).alias("running_max_quantity_filled"),
    )
)

result = (
    scored
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .cube(F.col("p.brand").alias("brand"), F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.running_min_quantity_filled")).alias("min_running_min_quantity_filled"),
        F.max(F.col("s.running_sum_quantity_filled")).alias("max_running_sum_quantity_filled"),
        F.grouping_id().alias("level_id"),
    )
)
