from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first(), "id")
w2 = Window.partitionBy("region").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("rank_in_group").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

latest = (
    df_store_sales
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    latest
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.rank_in_group",
        F.col("p.store_name"),
        F.col("p.sqft"),
        F.col("p.opened_date"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.rank_in_group",
        "s.store_name",
        F.col("b.store_name").alias("store_store_name"),
        F.col("b.region"),
    )
)

enriched = (
    with_dims
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("last_known_region_2", F.last("region", ignorenulls=True).over(w5))
)

result = (
    enriched
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "store_name",
        "region",
        "last_known_region",
        "row_seq",
        "event_seq",
        "last_known_region_2",
        F.col("max_inventory_quantity"),
    )
    .orderBy("region")
)
