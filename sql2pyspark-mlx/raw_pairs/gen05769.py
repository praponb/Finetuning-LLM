from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("product_brand").orderBy("rating", "id")
w3 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("rating").between(3.3, 4.0))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat("supplier_name", F.lit("/"), "city").alias("supplier_name_city"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") > 660), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating",
        "s.city_filled",
        F.col("c.brand").alias("product_brand"),
        F.col("c.category"),
    )
)

with_windows = (
    with_last
    .select(
        "id",
        "city_filled",
        "product_brand",
        "category",
        F.ntile(3).over(w2).alias("rating_bucket"),
        F.last("rating", ignorenulls=True).over(w3).alias("last_known_rating"),
    )
)

filtered = (
    with_windows
    .filter(F.col("category").isNotNull() & (~F.col("category").eqNullSafe(F.lit("Fashion"))))
    .withColumn("product_brand_category", F.concat("product_brand", F.lit(" "), "category"))
    .withColumn("category_group", F.when(F.col("category").isin(["Home", "Sports"]), "Group A").otherwise("Other"))
)

kept = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "bestseller")), "left_semi")
)

latest_match = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.category_group",
        F.col("c.stock_qty"),
        F.col("c.cost").alias("product_cost"),
    )
)

result = (
    latest_match
    .select("id", "category", "category_group", "stock_qty")
    .orderBy(F.col("stock_qty").desc(), F.col("id").desc())
)
