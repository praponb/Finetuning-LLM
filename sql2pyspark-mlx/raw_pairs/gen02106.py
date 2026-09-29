from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating_filled").desc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Pacif%"))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("supplier_name").like("%Sia%"))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

ranked = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "city", "is_us", "rating_filled")
)

base = (
    ranked
    .filter(F.col("city").isNotNull() & (F.col("is_us") < 74))
    .select(
        "*",
        (F.col("is_us") * F.col("rating_filled")).alias("is_us_times_rating_filled"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", "s.is_us", "s.rating_filled", F.col("c.price"))
)

valid = (
    with_last
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "id",
        "city",
        "is_us",
        "rating_filled",
        F.least("rating_filled", "is_us").alias("least_rating_filled_is_us"),
    )
)

result = (
    valid
    .select("id", "least_rating_filled_is_us")
    .orderBy(F.col("id").desc_nulls_first())
)
