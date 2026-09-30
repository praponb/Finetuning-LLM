from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "supplier_id").orderBy(F.col("supplier_rating").desc_nulls_last(), "id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("rating").desc(), "id")
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("stock_qty").alias("min_product_stock_qty"))

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-05-27")))
    .select(
        "id",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper("category").alias("category_upper"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

enriched = (
    with_last
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.supplier_id",
        F.col("d.country"),
        F.col("d.rating"),
        F.col("d.city"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.rating").alias("supplier_rating"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

deduped_products = (
    deduped
    .withColumn("dedup_rank_2", F.row_number().over(w3))
    .filter(F.col("dedup_rank_2") == 1)
    .select("id", "supplier_id", "country", "rating")
)

result = (
    deduped_products
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "supplier_id",
        "country",
        F.when(F.col("country").rlike("^(SG|TH)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
        F.col("min_product_stock_qty"),
    )
    .orderBy(F.col("min_product_stock_qty").desc())
)
