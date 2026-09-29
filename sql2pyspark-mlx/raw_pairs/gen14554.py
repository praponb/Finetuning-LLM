from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("years_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("running_sum_quantity", "id")
w6 = Window.partitionBy("store_id").orderBy("latest_rank", "id").rowsBetween(-5, -1)

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-07-08"))) & F.col("product_id").isin([1, 2, 3, 7]))
    .withColumn("years_since_last_restock", (F.datediff(F.current_date(), "last_restock") / 365))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .filter(F.year("last_restock") >= 2024)
    .select(
        "id",
        "store_id",
        "quantity",
        "latest_rank",
        F.sum("quantity").over(w2).alias("running_sum_quantity"),
        F.max("quantity").over(w2).alias("running_max_quantity"),
        F.min("quantity").over(w2).alias("running_min_quantity"),
        F.sum("years_since_last_restock").over(w3).alias("running_years_since_last_restock"),
    )
)

enriched_inventory = (
    enriched
    .withColumn("running_sum_quantity_avg_2", F.avg("running_sum_quantity").over(w4))
    .withColumn("running_sum_quantity_bucket", F.ntile(3).over(w5))
    .withColumn("running_sum_quantity_avg_5_prior", F.avg("running_sum_quantity").over(w6))
)

result = (
    enriched_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "running_sum_quantity",
        "running_max_quantity",
        "running_min_quantity",
        "running_years_since_last_restock",
        "running_sum_quantity_avg_2",
        "running_sum_quantity_bucket",
    )
    .orderBy("running_years_since_last_restock")
)
