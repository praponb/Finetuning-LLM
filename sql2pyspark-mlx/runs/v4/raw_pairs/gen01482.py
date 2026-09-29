from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w4 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty")

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("product_name").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.initcap(F.trim("category")).alias("category_title"),
        F.initcap(F.trim("brand")).alias("brand_title"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

valid_products = (
    valid
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.array_join("tags", "|").alias("tags_text"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

enriched = (
    valid_products
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cost_bucket", F.ntile(5).over(w2))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.last("cost", ignorenulls=True).over(w4).alias("last_known_cost"),
        F.dense_rank().over(w5).alias("stock_qty_dense_rank"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price") > 390), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "category_title",
        F.when(F.col("category").rlike("^(Electronics|Books)$"), "GROUP_A").when(F.col("category").rlike("^(Home|Sports)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy(F.col("price").desc_nulls_first(), F.col("id").desc())
)
