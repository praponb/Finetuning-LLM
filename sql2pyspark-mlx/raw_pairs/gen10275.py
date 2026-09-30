from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price_rounded", "id")
w2 = Window.partitionBy("supplier_city", "brand").orderBy("launch_date", "id")

with_dims = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select(
        "e.id",
        "e.category",
        "e.brand",
        "e.price",
        "e.stock_qty",
        "e.launch_date",
        "e.supplier_id",
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "inner")
    .filter(F.col("b.supplier_name").like("%Lio%"))
    .select(
        "e.*",
        F.col("b.supplier_name").alias("supplier_supplier_name"),
        F.col("b.rating").alias("supplier_rating"),
        F.col("b.city").alias("supplier_city"),
    )
)

filtered = (
    joined
    .filter(F.col("supplier_city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("New York"))))
    .withColumn("price_rounded", F.round("price", 1))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("NONE")))
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("rating").over(w).alias("prev_rating"),
        F.lead("rating").over(w).alias("next_rating"),
        F.lead("stock_qty").over(w).alias("next_stock_qty"),
        F.row_number().over(w).alias("row_seq"),
        F.lead("stock_qty", 2).over(w2).alias("next_stock_qty_2"),
    )
)

result = (
    enriched
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("supplier_rating")).otherwise(0)).alias("singapore_supplier_rating"),
        F.sum(F.when(F.col("category") == "Sports", F.col("rating")).otherwise(0)).alias("sports_rating"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("next_rating")).otherwise(0)).alias("electronics_next_rating"),
    )
)
