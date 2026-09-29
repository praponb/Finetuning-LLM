from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("launch_date").desc(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-08-29").cast("date")) & (F.col("quantity") > 113))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.quantity",
        "e.last_restock",
        F.col("b.launch_date"),
        F.col("b.brand"),
    )
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

valid = (
    ranked
    .filter(F.col("product_id") == 4)
    .select(
        "*",
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

result = (
    valid
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "launch_date",
        "launch_date_label",
        "resolved_brand",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
