from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("cost").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("stock_qty").alias("min_product_stock_qty"))

cleaned = (
    df_products
    .filter(F.col("category") == "Fashion")
    .select(
        "*",
        F.array_contains("tags", "eco").alias("is_eco"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

joined = (
    with_latest
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .filter(F.col("p2.city") == "London")
    .select("s.*", F.col("p2.rating"))
)

result = (
    joined
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        "rank_in_group",
        "unit_price",
        "rating",
        F.col("min_product_stock_qty"),
    )
    .orderBy(F.col("cost").desc())
)
