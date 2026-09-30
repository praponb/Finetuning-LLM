from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Berlin"))) & ~F.col("supplier_name").like("T%"))
    .select("*", F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"))
)

enriched = (
    filtered
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "supplier_name_normalized",
        F.lag("rating").over(w).alias("prev_rating"),
        F.lead("rating").over(w).alias("next_rating"),
        F.row_number().over(w).alias("seq_no"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.month(F.col("c.launch_date")).isin([3, 9, 10])), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

base = (
    latest_match
    .filter(F.col("supplier_name").like("%Toky%"))
    .select(
        "id",
        "city",
        "rating",
        "supplier_name_normalized",
        "prev_rating",
        "next_rating",
        "cost",
        F.round((F.col("rating") / F.col("seq_no")), 2).alias("rating_per_seq_no"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        "s.next_rating",
        "s.cost",
        "s.rating_per_seq_no",
        F.col("c.cost").alias("product_cost"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

result = (
    with_last
    .groupBy(F.date_trunc("week", "product_launch_date").alias("week_start"), "city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sum("next_rating").alias("total_next_rating"),
    )
    .filter(F.col("distinct_cities") >= 1)
    .orderBy(F.col("city").desc())
)
