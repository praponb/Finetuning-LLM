from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("price", "id")
w4 = Window.partitionBy("supplier_id").orderBy("stock_qty_whole", "id")
w5 = Window.partitionBy(F.month("last_restock"), "supplier_id").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("month", "last_restock"), "supplier_id").orderBy("last_restock", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-04-16").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.floor("stock_qty").alias("stock_qty_whole"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.price",
        "s.cost",
        "s.supplier_id",
        "s.stock_qty_whole",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

with_dims = (
    latest_match
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select("src.*", F.col("p.rating"), F.col("p.supplier_name"))
)

ranked = (
    with_dims
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

ranked_products = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked_products
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "supplier_id",
        "stock_qty_whole",
        "inventory_quantity",
        "rating",
        "supplier_name",
        F.lead("inventory_quantity", 2).over(w4).alias("next_inventory_quantity"),
        F.sum("rating").over(w5).alias("running_rating"),
        F.lag("rating", 1).over(w6).alias("prev_rating"),
    )
)

matched_keys = (
    enriched
    .select("id")
    .subtract(
        df_products
        .filter(F.col("price") >= 330)
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "product_name",
        "price",
        "cost",
        "supplier_id",
        "inventory_quantity",
        "rating",
        "supplier_name",
        "next_inventory_quantity",
        "running_rating",
        F.when(F.col("supplier_name").rlike("^(Lion City Imports|Global Supply)$"), "PRIORITY").when(F.col("supplier_name").rlike("^(Siam Parts Co|Tokyo Traders)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("supplier_name_zone"),
        F.col("avg_product_cost"),
    )
)
