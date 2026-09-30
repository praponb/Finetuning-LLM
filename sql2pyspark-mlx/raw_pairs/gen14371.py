from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("city_title", F.initcap(F.trim("city")))
)

metrics = (
    valid
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.max("rating").over(w).alias("last4_max_rating"),
        F.sum("rating").over(w).alias("last4_sum_rating"),
        F.count("rating").over(w).alias("last4_count_rating"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isNotNull()), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.category").alias("product_category"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

result = (
    with_latest
    .groupBy(F.date_trunc("YEAR", "product_launch_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("product_category") == "Grocery", 1)).alias("grocery_count"),
        F.percentile_approx("last4_count_rating", [0.25, 0.5, 0.75]).alias("last4_count_rating_quartiles"),
        F.count(F.when(F.col("product_category") == "Fashion", 1)).alias("fashion_rows"),
    )
    .filter(F.col("fashion_rows") > 1)
    .orderBy(F.col("fashion_rows").desc())
)
