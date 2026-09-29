from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("greatest_price_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "category").orderBy(F.col("supplier_rating").desc(), "id")

base = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-09-25"))) & F.upper("product_name").like("%UNN%"))
    .withColumn("product_name_category", F.concat("product_name", F.lit(" - "), "category"))
    .withColumn("greatest_price_stock_qty", F.greatest("price", "stock_qty"))
)

joined = (
    base
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id").eqNullSafe(F.col("s.id")), "left")
    .filter(F.col("s.rating") < 3.6)
    .select("a.*", F.col("s.rating"), F.col("s.city"))
)

joined_products = (
    joined
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.country"),
        F.col("b.rating").alias("supplier_rating"),
        F.col("b.city").alias("supplier_city"),
    )
)

with_windows = (
    joined_products
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "rating",
        "city",
        "country",
        "supplier_rating",
        "supplier_city",
        F.max("price").over(w).alias("country_max_price"),
        F.last("supplier_city", ignorenulls=True).over(w2).alias("last_known_supplier_city"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "product_name",
        "category",
        "rating",
        "supplier_rating",
        "country_max_price",
    )
    .orderBy(F.col("category").desc())
)
