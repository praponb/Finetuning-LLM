from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy(F.col("stock_qty").desc_nulls_last(), "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("cost").alias("max_product_cost"))

base = (
    df_products
    .filter(F.col("stock_qty") != 393)
    .withColumn("product_name_prefix", F.substring("product_name", 1, 2))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("DIRECT")))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "supplier_id",
        "product_name_prefix",
    )
)

kept_keys = (
    latest
    .select("id", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("brand").isNull())
        .select("id", "supplier_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") < "2024-02-01"), "left_semi")
)

enriched = (
    eligible
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.city"))
)

result = (
    enriched
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "supplier_id",
        "product_name_prefix",
        F.col("max_product_cost"),
    )
    .orderBy(F.col("price").desc(), F.col("brand").desc())
)
