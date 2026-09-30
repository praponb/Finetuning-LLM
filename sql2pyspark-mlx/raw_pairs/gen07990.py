from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date"))
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("launch_date").asc_nulls_first(), "id")

valid = (
    df_products
    .filter(F.col("price") <= 620)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.round("price", 0).alias("price_rounded"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .filter(F.lower(F.trim(F.col("p.supplier_name"))) == "lion city imports")
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.stock_qty",
        "src.launch_date",
        "src.tags",
        "src.supplier_id",
        "src.price_rounded",
        F.col("p.city"),
        F.col("p.rating"),
        F.col("p.supplier_name"),
    )
)

base = (
    joined
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "*",
        (F.col("rating") - F.col("price_rounded")).alias("rating_minus_price_rounded"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

metrics = (
    base
    .withColumn("launch_date_year_avg_price_rounded", F.avg("price_rounded").over(w))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w2))
    .withColumn("rating_bucket", F.ntile(3).over(w3))
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "brand",
        F.col("rating").alias("RATING"),
        "rating_minus_price_rounded",
        F.col("price_rounded").alias("PRICE_ROUNDED_VAL"),
        F.col("last_known_rating").alias("LAST_KNOWN_RATING"),
    )
    .unpivot(
        ids=["id", "brand"],
        values=["RATING", "rating_minus_price_rounded", "PRICE_ROUNDED_VAL", "LAST_KNOWN_RATING"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
