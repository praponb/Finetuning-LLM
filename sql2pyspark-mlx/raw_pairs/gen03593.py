from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id", "region").orderBy(F.col("quantity").asc_nulls_first())
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

scored = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]))
    .withColumn("prev_quantity", F.lag("quantity").over(w_ordered))
    .withColumn("next_quantity", F.lead("quantity").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_ordered))
)

filtered = (
    scored
    .filter(F.col("prev_quantity") >= 102)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "next_quantity",
        "seq_no",
        F.date_add("last_restock", 42).alias("last_restock_plus_42d"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.region"), F.col("b.store_name"))
)

latest = (
    with_dims
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "next_quantity",
        "seq_no",
        "region",
        "store_name",
        "rn",
        F.when(F.col("region").rlike("^(West|North)$"), "OTHER_ZONE").when(F.col("region").rlike("^(South|East)$"), "PRIORITY").otherwise("GROUP_B").alias("region_zone"),
        F.col("min_store_sale_units"),
    )
)
