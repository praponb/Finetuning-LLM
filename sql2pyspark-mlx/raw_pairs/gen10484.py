from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("country") != "SG")
    .select(
        "id",
        "supplier_name",
        "city",
        F.lower("city").alias("city_lower"),
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

valid = (
    base
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "city_clean",
        F.concat_ws(",", "supplier_name", "city").alias("supplier_name_city_key"),
        F.lower("supplier_name").alias("supplier_name_lower"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.stock_qty").alias("product_stock_qty"),
        F.col("c.cost").alias("product_cost"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.city_clean",
        "s.supplier_name_city_key",
        "s.resolved_city",
        "s.product_stock_qty",
        "s.product_cost",
        F.col("c.cost").alias("product_cost_2"),
        F.col("c.price").alias("product_price"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "city",
        "city_clean",
        "supplier_name_city_key",
        "resolved_city",
        "product_stock_qty",
        "product_cost_2",
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
