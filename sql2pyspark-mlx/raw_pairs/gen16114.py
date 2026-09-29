from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

metrics = (
    base
    .filter(F.col("country") == "DE")
    .select(
        "*",
        F.lead("country_length").over(w_ordered).alias("next_country_length"),
        F.lag("rating").over(w_ordered).alias("prev_rating"),
        F.lead("rating").over(w_ordered).alias("next_rating"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

valid = (
    metrics
    .filter(F.col("seq_no") != 5)
    .withColumn("next_rating_rounded", F.round("next_rating", 2))
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "eco")), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost").between(230, 690)), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.stock_qty").alias("product_stock_qty"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

kept_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= F.col("s.product_launch_date")), "left_anti")
)

result = (
    kept_suppliers
    .groupBy(
        F.when(F.col("prev_rating") >= 4.3, "LARGE").otherwise("SMALL").alias("prev_rating_tier"),
        F.date_trunc("year", "product_launch_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("country_length", 0.5).alias("country_length_p50"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("next_rating"))).alias("chiang_mai_total_next_rating"),
        F.percentile_approx("next_country_length", [0.25, 0.5, 0.75]).alias("next_country_length_quartiles"),
    )
    .orderBy(F.col("chiang_mai_total_next_rating").desc_nulls_first())
)
