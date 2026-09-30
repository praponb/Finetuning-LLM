from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_first(), "id")

filtered = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "tags",
        F.coalesce("product_name", F.lit("unassigned")).alias("product_name_filled"),
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("product_name").like("R%"))
    .select("*", F.substring("category", 1, 3).alias("category_prefix"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id") == 4), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < "2023-09-20"), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "product_name",
        "cost",
        "stock_qty",
        "tags",
        "brand_clean",
        "category_prefix",
        "rn",
    )
    .orderBy(F.col("stock_qty").asc_nulls_first(), "rn")
)
