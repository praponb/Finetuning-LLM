from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("adjusted_quantity").desc())
w2 = Window.partitionBy("product_id").orderBy("adjusted_quantity", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("adjusted_quantity_avg_5_prior").desc_nulls_last(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-06-30")))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.round((F.col("quantity") * 100), 2).alias("adjusted_quantity"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "product_id",
        "last_restock",
        F.cume_dist().over(w).alias("adjusted_quantity_cume_dist"),
        F.avg("adjusted_quantity").over(w2).alias("adjusted_quantity_avg_5_prior"),
        F.first("adjusted_quantity", ignorenulls=True).over(w3).alias("first_known_adjusted_quantity"),
    )
)

base_inventory = (
    with_windows
    .filter((F.col("last_restock") >= F.lit("2023-04-17").cast("date")) & (F.col("first_known_adjusted_quantity") >= 43))
    .withColumn("adjusted_quantity_avg_5_prior_filled", F.coalesce("adjusted_quantity_avg_5_prior", F.lit(0)))
)

deduped = (
    base_inventory
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock",
        "adjusted_quantity_cume_dist",
        "adjusted_quantity_avg_5_prior",
        "first_known_adjusted_quantity",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("first_known_adjusted_quantity").desc_nulls_last())
)
