from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("city_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w5 = Window.orderBy("least_prev_city_length_per_rating_prev_city_length", "id")
w6 = Window.orderBy("prev_city_length_per_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("prev_city_length", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.length("city").alias("city_length"),
    )
)

scored = (
    cleaned
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("prev_city_length", F.lag("city_length", 1).over(w3))
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") >= 510), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.rating",
        "s.first_known_city",
        "s.event_seq",
        "s.prev_city_length",
        F.col("c.category").alias("product_category"),
    )
)

cleaned_suppliers = (
    latest_match
    .filter(F.col("product_category").isin(["Sports", "Electronics"]))
    .select(
        "id",
        "supplier_name",
        "rating",
        "first_known_city",
        "event_seq",
        "prev_city_length",
        F.round((F.col("prev_city_length") / F.col("rating")), 2).alias("prev_city_length_per_rating"),
        F.coalesce("product_category", F.lit("UNKNOWN")).alias("resolved_product_category"),
    )
)

filtered = (
    cleaned_suppliers
    .filter(F.col("prev_city_length_per_rating") < 21)
    .select(
        "id",
        "supplier_name",
        "rating",
        "first_known_city",
        "prev_city_length",
        "prev_city_length_per_rating",
        "resolved_product_category",
        F.least("prev_city_length_per_rating", "prev_city_length").alias("least_prev_city_length_per_rating_prev_city_length"),
    )
)

result = (
    filtered
    .filter(F.col("prev_city_length_per_rating") < 51)
    .select(
        "*",
        F.ntile(3).over(w5).alias("least_prev_city_length_per_rating_prev_city_length_bucket"),
        F.sum("prev_city_length_per_rating").over(w6).alias("cumulative_prev_city_length_per_rating"),
        F.lead("prev_city_length_per_rating", 2).over(w7).alias("next_prev_city_length_per_rating"),
    )
)
