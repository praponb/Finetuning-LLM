from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("id", "country", "city", "rating", F.upper("city").alias("city_upper"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        "s.city_upper",
        F.col("c.cost"),
        F.col("c.launch_date"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city_upper",
        "s.cost",
        "s.launch_date",
        F.col("c.category"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

remaining_keys = (
    with_last
    .select("country", "id")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "DE")
        .select("country", "id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    verified
    .groupBy("category")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
    )
)

result = (
    summary
    .select(
        "category",
        "distinct_countries",
        "de_rows",
        F.when(F.col("category").rlike("^(Home|Books)$"), "GROUP_A").when(F.col("category").rlike("^(Electronics|Sports)$"), "CORE").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy(F.col("de_rows").asc_nulls_last())
)
