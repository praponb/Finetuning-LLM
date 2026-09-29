from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("quantity_filled", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc())

valid = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-04-01")
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

metrics = (
    valid
    .select(
        "*",
        F.ntile(5).over(w).alias("quantity_filled_bucket"),
        F.lag("quantity", 1, 0).over(w2).alias("prev_quantity"),
        F.first("quantity", ignorenulls=True).over(w3).alias("first_known_quantity"),
    )
)

valid_inventory = (
    metrics
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .select(
        "*",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

ranked = (
    valid_inventory
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_filled",
        "quantity_filled_bucket",
        "prev_quantity",
        "days_since_last_restock",
    )
)

result = (
    ranked
    .groupBy(
        "store_id",
        F.when(F.col("quantity_filled") >= 62, "LARGE").otherwise("SMALL").alias("quantity_filled_tier"),
    )
    .agg(
        F.percentile_approx("days_since_last_restock", 0.95).alias("days_since_last_restock_p95"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity_filled_bucket", 0.5).alias("quantity_filled_bucket_p50"),
    )
)
