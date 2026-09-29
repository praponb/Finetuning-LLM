from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city") != "Berlin")
    .select(
        "id",
        "city",
        F.substring("city", 1, 3).alias("city_prefix"),
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Books", "Fashion"])), "left_anti")
)

valid_suppliers = (
    eligible
    .filter(F.col("city").isNotNull())
    .withColumn("city_lower", F.lower("city"))
)

with_last = (
    valid_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.city_title",
        "s.city_lower",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.lower(F.trim(F.col("c.product_name"))) == "water bottle"), "left_semi")
)

result = (
    not_flagged
    .select("id", "city_title")
    .orderBy(F.col("id").desc())
    .limit(10)
)
