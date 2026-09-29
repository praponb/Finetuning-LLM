from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("rating").desc_nulls_last(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "city",
        "rating",
        F.trim("country").alias("clean_country"),
        F.trim("city").alias("clean_city"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        "s.clean_country",
        "s.clean_city",
        F.col("c.category"),
        F.col("c.brand").alias("product_brand"),
    )
)

latest = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

eligible = (
    latest
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isNotNull()), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"), F.col("c.category").alias("product_category"))
)

valid = (
    with_latest
    .filter(F.col("category").isNotNull() & (F.col("product_category") == "Grocery"))
    .select(
        "id",
        "city",
        "clean_country",
        "clean_city",
        "category",
        "product_brand",
        "price",
        "product_category",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("price") >= 1180, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("num_cities").desc_nulls_last())
)
