from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_last
    .withColumn("cost_tier", F.when(F.col("cost") >= 670, "LARGE").otherwise("SMALL"))
    .groupingSets([["cost_tier", "brand"], ["cost_tier"]], "cost_tier", "brand")
    .agg(
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.count_if(F.col("category") == "Grocery").alias("grocery_events"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("sports_events") > 1)
)
