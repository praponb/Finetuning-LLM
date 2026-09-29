from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("last_restock_month_end", "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("prev_quantity").asc_nulls_first())
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("units").alias("max_store_sale_units"))

valid = (
    df_inventory
    .filter(F.col("last_restock").between("2023-10-01", "2025-10-05"))
    .select("*", F.last_day("last_restock").alias("last_restock_month_end"))
)

with_windows = (
    valid
    .select(
        "id",
        "product_id",
        "last_restock_month_end",
        F.lead("quantity").over(w_ordered).alias("next_quantity"),
        F.lag("quantity").over(w_ordered).alias("prev_quantity"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

kept_keys = (
    with_windows
    .select("id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([3, 5, 7, 9]))
        .select("id", "product_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"))
)

valid_inventory = (
    joined
    .filter(F.col("category").isNotNull())
    .withColumn("category_lower", F.lower("category"))
    .withColumn("prev_quantity_scaled", F.round((F.col("prev_quantity") * 100), 2))
)

with_windows_inventory = (
    valid_inventory
    .filter(F.col("prev_quantity").between(165, 202))
    .withColumn("product_id_avg_next_quantity", F.avg("next_quantity").over(w2))
    .withColumn("product_id_max_prev_quantity", F.max("prev_quantity").over(w2))
    .withColumn("prev_quantity_rank", F.rank().over(w3))
)

result = (
    with_windows_inventory
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "event_seq",
        "category",
        "prev_quantity_scaled",
        "product_id_max_prev_quantity",
        "prev_quantity_rank",
        F.when(F.col("category").rlike("^(Grocery|Sports)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("category_zone"),
        F.col("max_store_sale_units"),
    )
    .orderBy("category_zone")
)
