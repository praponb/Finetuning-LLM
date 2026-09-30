from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("city").orderBy("rating", "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.trim("city").alias("clean_city"),
        F.round((F.col("rating") * 1.15), 2).alias("adjusted_rating"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

base = (
    valid
    .filter(F.col("rating") > 2.7)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_filled",
        F.trim("city").alias("clean_city_2"),
        F.regexp_replace("supplier_name", " ", "-").alias("supplier_name_slug"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

with_windows = (
    base
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w))
    .withColumn("rating_avg_5", F.avg("rating").over(w2))
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("rating_avg_5").over(w3).alias("country_avg_rating_avg_5"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("rating_avg_5") >= 3, "LARGE").otherwise("SMALL").alias("rating_avg_5_tier"),
        "seq_no",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
    .filter(F.col("num_rows") > 1)
)

result = (
    daily
    .select("rating_avg_5_tier", "seq_no", "num_rows", "rating_quartiles", "avg_seq_no")
    .orderBy(F.col("avg_seq_no").desc_nulls_first())
)
