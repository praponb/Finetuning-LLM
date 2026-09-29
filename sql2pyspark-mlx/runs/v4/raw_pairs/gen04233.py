from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("supplier_city_2").orderBy(F.col("supplier_rating").desc_nulls_first())

joined = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.rating"), F.col("p.city"))
)

joined_products = (
    joined
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city").alias("supplier_city"))
)

remaining_keys = (
    joined_products
    .select("supplier_id")
    .intersect(
        df_products
        .filter(F.col("cost").isNull())
        .select("supplier_id")
    )
)

reconciled = (
    joined_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("launch_date") > F.lit("2024-03-19").cast("date")) & (F.col("brand") != "Stark"))
    .withColumn("clean_brand", F.trim("brand"))
    .withColumn("margin", (F.col("price") - F.col("cost")))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("DIRECT")))
)

valid = (
    cleaned
    .filter((F.col("launch_date") > F.lit("2023-08-10").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "rating",
        "city",
        "supplier_city",
        "clean_brand",
        "resolved_brand",
        F.greatest("margin", "rating").alias("greatest_margin_rating"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.supplier_name"),
        F.col("p.rating").alias("supplier_rating"),
        F.col("p.city").alias("supplier_city_2"),
    )
)

enriched = (
    with_dims
    .select(
        "*",
        F.corr("price", "cost").over(w).alias("corr_price_cost"),
        F.percent_rank().over(w2).alias("supplier_rating_percent_rank"),
    )
)

result = (
    enriched
    .groupBy("city")
    .pivot("supplier_city", ["New York", "Bangkok", "Singapore"])
    .agg(F.avg("greatest_margin_rating").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "city",
        F.col("New York_total").alias("new_york_total"),
        F.col("New York_n").alias("new_york_n"),
        F.col("Bangkok_total").alias("bangkok_total"),
        F.col("Bangkok_n").alias("bangkok_n"),
        F.col("Singapore_total").alias("singapore_total"),
        F.col("Singapore_n").alias("singapore_n"),
    )
)
