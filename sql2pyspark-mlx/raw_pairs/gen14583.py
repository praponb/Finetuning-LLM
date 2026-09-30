from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("cumulative_supplier_name_length", "id")
w4 = Window.partitionBy("city").orderBy(F.col("prev_supplier_name_length").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w6 = Window.orderBy("price", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.lower("supplier_name").like("%ion%"))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

scored = (
    base
    .select(
        "*",
        F.lead("rating").over(w).alias("next_rating"),
        F.lead("supplier_name_length").over(w).alias("next_supplier_name_length"),
        F.lag("supplier_name_length").over(w).alias("prev_supplier_name_length"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("supplier_name_length").over(w2).alias("cumulative_supplier_name_length"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "city",
        "rating",
        "next_rating",
        "next_supplier_name_length",
        "prev_supplier_name_length",
    )
)

latest_suppliers = (
    latest
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    latest_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.next_supplier_name_length",
        F.col("c.price"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

valid = (
    latest_match
    .filter(F.col("city").isNull())
    .select(
        "id",
        "price",
        F.upper("city").alias("city_upper"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

metrics = (
    valid
    .filter(F.col("price") >= 570)
    .select(
        "id",
        "price",
        "city_upper",
        F.min("price").over(w6).alias("last3_min_price"),
        F.sum("price").over(w6).alias("last3_sum_price"),
        F.avg("price").over(w6).alias("last3_avg_price"),
        F.count("price").over(w6).alias("last3_count_price"),
    )
)

result = (
    metrics
    .select(
        "id",
        "price",
        "city_upper",
        "last3_min_price",
        "last3_avg_price",
        "last3_count_price",
    )
)
