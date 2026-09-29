from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("product_category", "country").orderBy("product_stock_qty", "id")

cleaned = (
    df_suppliers
    .filter(F.col("rating").between(2.7, 3.4))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.upper("country").alias("country_upper"),
    )
)

filtered = (
    cleaned
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "supplier_name",
        "country",
        "country_upper",
        F.substring("supplier_name", 1, 3).alias("supplier_name_prefix"),
        F.substring("country", 1, 2).alias("country_prefix"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.category").alias("product_category"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "sale")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "supplier_name_prefix",
        F.lead("product_stock_qty").over(w_hist).alias("next_product_stock_qty"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)
