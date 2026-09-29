from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date_period", "id")
w2 = Window.partitionBy("category").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("stock_qty").asc_nulls_last())

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("stock_qty").between(117, 328))
    .withColumn("category_product_name", F.concat("category", F.lit(" "), "product_name"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("OTHER")))
)

cleaned_products = (
    cleaned
    .filter((~F.col("brand").eqNullSafe(F.lit("Globex"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "resolved_brand",
        F.date_trunc("week", "launch_date").alias("launch_date_period"),
    )
)

flagged = (
    cleaned_products
    .withColumn("session_start_flag", F.when(F.lag("launch_date_period").over(w).isNull() | (F.col("launch_date_period") > F.lag("launch_date_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

kept_keys = (
    sessionized
    .select("brand")
    .subtract(
        df_products
        .filter(F.col("brand").isNull())
        .select("brand")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

enriched = verified.select("*", F.cume_dist().over(w3).alias("stock_qty_cume_dist"))

result = (
    enriched
    .select(
        "category",
        "brand",
        "stock_qty",
        "tags",
        "supplier_id",
        "island_id",
        "stock_qty_cume_dist",
        F.when(F.col("category").rlike("^(Fashion|Grocery)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("category_zone"),
    )
    .orderBy(F.col("island_id").desc_nulls_last())
)
